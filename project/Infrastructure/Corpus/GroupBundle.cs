using System.Security.Cryptography;

namespace S3SyncLab.Infrastructure.Corpus;

public sealed record GroupSourceFile(
    string RelativePath,
    string FullPath,
    string? SymbolicLinkTarget = null,
    bool Executable = false);

public sealed record GroupBundleInfo(
    int Files,
    long ContentBytes,
    long BundleBytes);

// A deterministic, uncompressed container: paths, lengths, SHA-256 and file bytes.
// Timestamps and directory enumeration order cannot change its binary contents.
public static class GroupBundle
{
    private const int MaximumFiles = 65_536;
    private const int MaximumPathBytes = 4096;
    private const int DigestBytes = 32;
    private const byte RegularFile = 0;
    private const byte SymbolicLink = 1;
    private const byte ExecutableFile = 2;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("S3GRP002");
    private static readonly UTF8Encoding Utf8 = new(false, true);

    public static GroupBundleInfo Write(
        IEnumerable<GroupSourceFile> sources,
        string outputFile,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        var entries = sources
            .OrderBy(item => item.RelativePath, StringComparer.Ordinal)
            .ToArray();

        if (entries.Length > MaximumFiles)
        {
            throw new InvalidDataException("Too many files in the group.");
        }

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        long expectedBytes = Magic.Length + sizeof(int);
        long contentBytes = 0;

        foreach (var entry in entries)
        {
            ValidateRelativePath(entry.RelativePath);

            if (!paths.Add(entry.RelativePath))
            {
                throw new InvalidDataException("Duplicate or case-colliding group path.");
            }

            if (entry.SymbolicLinkTarget is null)
            {
                EnsureRegularFile(entry.FullPath);
            }
            else
            {
                ValidateLinkTarget(entry.RelativePath, entry.SymbolicLinkTarget);
            }

            long length = entry.SymbolicLinkTarget is null
                ? new FileInfo(entry.FullPath).Length
                : Utf8.GetByteCount(entry.SymbolicLinkTarget);

            contentBytes = checked(contentBytes + length);
            expectedBytes = checked(
                expectedBytes
                    + sizeof(byte)
                    + sizeof(int)
                    + Utf8.GetByteCount(entry.RelativePath)
                    + sizeof(long)
                    + DigestBytes
                    + length
            );
        }

        foreach (var link in entries.Where(entry => entry.SymbolicLinkTarget is not null))
        {
            if (paths.Any(path => path.StartsWith(link.RelativePath + "/", StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidDataException("A group entry cannot be a child of a symbolic link.");
            }
        }

        if (expectedBytes > maximumBytes)
        {
            throw new InvalidDataException("Group exceeds maximumGroupBytes.");
        }

        string temporary = outputFile + "." + Guid.NewGuid().ToString("N") + ".tmp";
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(outputFile))!);
        var buffer = new byte[FileOperations.BufferBytes];

        try
        {
            using (var output = new FileStream(
                temporary,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                FileOperations.BufferBytes))
            using (var writer = new BinaryWriter(output, Utf8, leaveOpen: true))
            {
                writer.Write(Magic);
                writer.Write(entries.Length);

                foreach (var entry in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using Stream input = entry.SymbolicLinkTarget is null
                        ? FileOperations.OpenRead(entry.FullPath)
                        : new MemoryStream(Utf8.GetBytes(entry.SymbolicLinkTarget), writable: false);

                    byte[] name = Utf8.GetBytes(entry.RelativePath);

                    writer.Write(
                        entry.SymbolicLinkTarget is not null
                            ? SymbolicLink
                            : entry.Executable
                                ? ExecutableFile
                                : RegularFile
                    );

                    writer.Write(name.Length);
                    writer.Write(name);
                    writer.Write(input.Length);

                    long digestOffset = output.Position;
                    writer.Write(new byte[DigestBytes]);

                    using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                    long copied = 0;
                    int count;

                    while ((count = input.Read(buffer, 0, buffer.Length)) != 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        output.Write(buffer, 0, count);
                        hash.AppendData(buffer, 0, count);
                        copied = checked(copied + count);
                    }

                    if (copied != input.Length)
                    {
                        throw new IOException("A source file changed during packaging.");
                    }

                    long end = output.Position;
                    output.Position = digestOffset;
                    writer.Write(hash.GetHashAndReset());
                    output.Position = end;
                }

                if (output.Length != expectedBytes)
                {
                    throw new IOException("The group changed during packaging.");
                }

                writer.Flush();
                output.Flush(flushToDisk: true);
            }

            File.Move(temporary, outputFile, overwrite: true);

            return new GroupBundleInfo(entries.Length, contentBytes, expectedBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
            FileOperations.DeleteIfExists(temporary);
        }
    }

    public static GroupBundleInfo Extract(
        string bundleFile,
        string newDirectory,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        if (Directory.Exists(newDirectory)
            || File.Exists(newDirectory))
        {
            throw new IOException("Group extraction requires a new directory.");
        }

        using var input = FileOperations.OpenRead(bundleFile);

        if (input.Length > maximumBytes)
        {
            throw new InvalidDataException("Group container exceeds its size limit.");
        }

        using var reader = new BinaryReader(input, Utf8, leaveOpen: true);
        byte[] magic = reader.ReadBytes(Magic.Length);

        if (!magic.SequenceEqual(Magic))
        {
            throw new InvalidDataException("Unknown group container format.");
        }

        int files = reader.ReadInt32();

        if (files < 0
            || files > MaximumFiles)
        {
            throw new InvalidDataException("Invalid group file count.");
        }

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var links = new List<(string Path, string Target)>();
        string? previous = null;
        long contentBytes = 0;
        var buffer = new byte[FileOperations.BufferBytes];

        Directory.CreateDirectory(newDirectory);

        try
        {
            for (int index = 0; index < files; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                byte kind = reader.ReadByte();

                if (kind is not (RegularFile or SymbolicLink or ExecutableFile))
                {
                    throw new InvalidDataException("Unsupported group entry type.");
                }

                int nameLength = reader.ReadInt32();

                if (nameLength < 1
                    || nameLength > MaximumPathBytes)
                {
                    throw new InvalidDataException("Invalid path length.");
                }

                byte[] nameBytes = reader.ReadBytes(nameLength);

                if (nameBytes.Length != nameLength)
                {
                    throw new EndOfStreamException();
                }

                string name = Utf8.GetString(nameBytes);
                ValidateRelativePath(name);

                if (!paths.Add(name)
                    || (previous is not null && StringComparer.Ordinal.Compare(previous, name) >= 0))
                {
                    throw new InvalidDataException("Group paths must be unique and sorted.");
                }

                previous = name;
                long length = reader.ReadInt64();
                byte[] expectedHash = reader.ReadBytes(DigestBytes);

                if (length < 0
                    || expectedHash.Length != DigestBytes
                    || length > input.Length - input.Position)
                {
                    throw new InvalidDataException("Invalid group file length.");
                }

                contentBytes = checked(contentBytes + length);

                if (contentBytes > maximumBytes)
                {
                    throw new InvalidDataException("Group content exceeds its size limit.");
                }

                if (kind == SymbolicLink)
                {
                    if (length > MaximumPathBytes)
                    {
                        throw new InvalidDataException("Symbolic link target is too long.");
                    }

                    byte[] targetBytes = reader.ReadBytes((int)length);

                    if (targetBytes.Length != length)
                    {
                        throw new EndOfStreamException();
                    }

                    if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(targetBytes), expectedHash))
                    {
                        throw new InvalidDataException("Symbolic link SHA-256 mismatch.");
                    }

                    string target = Utf8.GetString(targetBytes);
                    ValidateLinkTarget(name, target);
                    links.Add((name, target));

                    continue;
                }

                string destination = Resolve(newDirectory, name);
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);

                using var output = new FileStream(
                    destination,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    FileOperations.BufferBytes
                );

                using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                long remaining = length;

                while (remaining > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int count = input.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));

                    if (count == 0)
                    {
                        throw new EndOfStreamException();
                    }

                    output.Write(buffer, 0, count);
                    hash.AppendData(buffer, 0, count);
                    remaining -= count;
                }

                if (!CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), expectedHash))
                {
                    throw new InvalidDataException("Restored group file SHA-256 mismatch: " + name);
                }

                if (kind == ExecutableFile
                    && !OperatingSystem.IsWindows())
                {
                    File.SetUnixFileMode(
                        destination,
                        UnixFileMode.UserRead
                            | UnixFileMode.UserWrite
                            | UnixFileMode.UserExecute
                            | UnixFileMode.GroupRead
                            | UnixFileMode.GroupExecute
                            | UnixFileMode.OtherRead
                            | UnixFileMode.OtherExecute
                    );
                }
            }

            if (input.Position != input.Length)
            {
                throw new InvalidDataException("Unexpected group container trailer.");
            }

            // Links are created last, after all regular writes have finished. No entry may
            // place another file or link below a link path.
            foreach (var link in links)
            {
                if (paths.Any(path => path.StartsWith(link.Path + "/", StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidDataException("A group entry cannot be a child of a symbolic link.");
                }
            }

            foreach (var link in links)
            {
                string destination = Resolve(newDirectory, link.Path);
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
                File.CreateSymbolicLink(destination, link.Target);
            }

            return new GroupBundleInfo(files, contentBytes, input.Length);
        }
        catch
        {
            Directory.Delete(newDirectory, recursive: true);
            throw;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }

    public static string Resolve(string root, string relativePath)
    {
        ValidateRelativePath(relativePath);

        return Path.Combine(
            Path.GetFullPath(root),
            relativePath.Replace('/', Path.DirectorySeparatorChar)
        );
    }

    public static void ValidateRelativePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path)
            || path.Length > MaximumPathBytes
            || Utf8.GetByteCount(path) > MaximumPathBytes
            || path.StartsWith('/')
            || path.Contains('\\')
            || path.Any(c => char.IsControl(c) || "<>:\"|?*".Contains(c)))
        {
            throw new InvalidDataException("Unsafe or nonportable group path.");
        }

        foreach (string segment in path.Split('/'))
        {
            string stem = segment.Split('.')[0].ToUpperInvariant();

            if (segment.Length == 0
                || segment is "." or ".."
                || segment.EndsWith('.')
                || segment.EndsWith(' ')
                || segment.Equals(".git", StringComparison.OrdinalIgnoreCase)
                || stem is "CON" or "PRN" or "AUX" or "NUL"
                || (stem.Length == 4
                    && (stem.StartsWith("COM") || stem.StartsWith("LPT"))
                    && stem[3] is >= '1' and <= '9'))
            {
                throw new InvalidDataException("Unsafe or reserved group path segment.");
            }
        }
    }

    public static void EnsureRegularFile(string path)
    {
        var attributes = File.GetAttributes(path);

        if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != 0)
        {
            throw new InvalidDataException("Groups require regular files, without symbolic links.");
        }
    }

    public static void ValidateLinkTarget(string entryPath, string target)
    {
        if (string.IsNullOrWhiteSpace(target)
            || Utf8.GetByteCount(target) > MaximumPathBytes
            || target.StartsWith('/')
            || target.Contains('\\')
            || target.Contains(':')
            || target.Any(char.IsControl))
        {
            throw new InvalidDataException("Symbolic link target must be a relative path inside the group.");
        }

        string root = Path.Combine(Path.GetTempPath(), "s3sync-link-validation-root");
        string parent = Path.GetDirectoryName(Resolve(root, entryPath))!;
        string resolved = Path.GetFullPath(target.Replace('/', Path.DirectorySeparatorChar), parent);
        string relative = Path.GetRelativePath(root, resolved);

        if (relative == ".."
            || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || Path.IsPathRooted(relative))
        {
            throw new InvalidDataException("Symbolic link target escapes the group.");
        }
    }
}