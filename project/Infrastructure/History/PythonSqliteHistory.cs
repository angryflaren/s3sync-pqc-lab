using System.Text.Json;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.History;

// SQL updates remain in the existing dataset generator; the synchronization code
// depends only on IVersionHistory and can later read a different type of history.
public sealed class PythonSqliteHistory : IVersionHistory
{
    private readonly string dataset;
    private readonly string work;
    private readonly string python;
    private readonly string script;
    private readonly int updates;
    private readonly int timeoutSeconds;
    private int version;

    public string CurrentFile => Path.Combine(work, "working.sqlite");

    public PythonSqliteHistory(
        string dataset,
        string work,
        string python,
        string script,
        int updates,
        int timeoutSeconds)
    {
        this.dataset = dataset;
        this.work = work;
        this.python = python;
        this.script = script;
        this.updates = updates;
        this.timeoutSeconds = timeoutSeconds;
    }

    public async Task<DatasetInfo> InitializeAsync(CancellationToken cancellationToken)
    {
        if (Directory.Exists(work))
        {
            throw new IOException("History workspace already exists: " + work);
        }

        if (!File.Exists(script))
        {
            throw new FileNotFoundException("Set historyScript in the configuration.", script);
        }

        string manifestFile = Path.Combine(dataset, "manifest.json");
        string historyFile = Path.Combine(dataset, "changes.jsonl");

        using var manifest = JsonDocument.Parse(File.ReadAllText(manifestFile));
        var data = manifest.RootElement;

        var expected = new FileFingerprint(
            data.GetProperty("bytes").GetInt64(),
            data.GetProperty("sha256").GetString()!.ToLowerInvariant());

        int rows = data.GetProperty("rows").GetInt32();
        int declaredUpdates = data.GetProperty("updates").GetInt32();

        _ = data.GetProperty("data_seed").GetString() ?? throw new InvalidDataException("Missing data_seed.");

        if (updates > declaredUpdates)
        {
            throw new InvalidDataException("Requested more updates than the dataset contains.");
        }

        if (data.TryGetProperty("file", out var name)
            && name.GetString() != "initial.sqlite")
        {
            throw new InvalidDataException("This adapter expects initial.sqlite.");
        }

        string runtime = await ExecuteAsync(
            new[]
            {
                "-c",
                "import json,platform,sqlite3,runpy,sys; m=runpy.run_path(sys.argv[1]); " +
                "print(json.dumps({'python':platform.python_version(),'sqlite':sqlite3.sqlite_version," +
                "'rows':m.get('ROWS'),'steps':m.get('STEPS'),'changedRows':m.get('CHANGED_ROWS')}))",
                script
            },
            cancellationToken);

        using var runtimeInfo = JsonDocument.Parse(runtime);

        if (runtimeInfo.RootElement.GetProperty("rows").GetInt32() != rows
            || runtimeInfo.RootElement.GetProperty("steps").GetInt32() < updates)
        {
            throw new InvalidDataException("The history script and manifest describe different datasets.");
        }

        int changedRows = runtimeInfo.RootElement.GetProperty("changedRows").GetInt32();
        int count = 0;

        foreach (string line in File.ReadLines(historyFile))
        {
            using var operation = JsonDocument.Parse(line);
            var item = operation.RootElement;

            if (item.GetProperty("step").GetInt32() != ++count
                || item.GetProperty("action").GetString() != "update_rows"
                || item.GetProperty("row_count").GetInt32() != changedRows
                || item.GetProperty("first_row").GetInt32() < 0
                || item.GetProperty("first_row").GetInt32() >= rows
                || item.GetProperty("stride").GetInt32() < 1)
            {
                throw new InvalidDataException("Invalid or incompatible changes.jsonl record.");
            }
        }

        if (count != declaredUpdates)
        {
            throw new InvalidDataException("History length differs from manifest.updates.");
        }

        string initial = Path.Combine(dataset, "initial.sqlite");
        FileOperations.RequireMatch(initial, expected, cancellationToken);

        Directory.CreateDirectory(work);
        File.Copy(manifestFile, Path.Combine(work, "manifest.json"));
        File.Copy(historyFile, Path.Combine(work, "changes.jsonl"));

        await FileOperations.CopyAtomicAsync(initial, CurrentFile, cancellationToken);
        FileOperations.RequireMatch(CurrentFile, expected, cancellationToken);

        FileOperations.WriteJsonAtomic(
            Path.Combine(work, "run_state.json"),
            new
            {
                step = 0
            });

        version = 0;

        return new DatasetInfo(
            data.GetProperty("dataset").GetString() ?? "single-file",
            declaredUpdates,
            expected,
            FileOperations.Fingerprint(manifestFile, cancellationToken).Sha256,
            FileOperations.Fingerprint(historyFile, cancellationToken).Sha256,
            runtime + "; script-sha256=" + FileOperations.Fingerprint(script, cancellationToken).Sha256);
    }

    public async Task AdvanceAsync(int expectedVersion, CancellationToken cancellationToken)
    {
        if (expectedVersion != version + 1
            || expectedVersion > updates)
        {
            throw new InvalidOperationException("History must be replayed sequentially.");
        }

        await ExecuteAsync(new[] { script, "--root", work, "next" }, cancellationToken);

        using var state = JsonDocument.Parse(File.ReadAllText(Path.Combine(work, "run_state.json")));

        if (state.RootElement.GetProperty("step").GetInt32() != expectedVersion)
        {
            throw new InvalidDataException("Python did not prepare the requested version.");
        }

        // Never take a binary snapshot while SQLite still has an uncheckpointed WAL.
        if (File.Exists(CurrentFile + "-wal")
            || File.Exists(CurrentFile + "-journal"))
        {
            throw new InvalidDataException("SQLite journal remains open after the update.");
        }

        version = expectedVersion;
    }

    private async Task<string> ExecuteAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo(python)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8
            }
        };

        foreach (string argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

        process.Start();

        Task<string> stdout = process.StandardOutput.ReadToEndAsync();
        Task<string> stderr = process.StandardError.ReadToEndAsync();

        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            try
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
            catch (InvalidOperationException) when (process.HasExited)
            {
            }

            await process.WaitForExitAsync(CancellationToken.None);
            await Task.WhenAll(stdout, stderr);

            cancellationToken.ThrowIfCancellationRequested();

            throw new TimeoutException("Python history preparation exceeded its configured timeout.");
        }

        string output = await stdout;
        string error = await stderr;

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException("History script failed: " + error.Trim());
        }

        return output.Trim();
    }
}