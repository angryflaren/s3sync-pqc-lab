using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using S3SyncLab.Configuration;
using S3SyncLab.Core;

namespace S3SyncLab.Infrastructure.Storage;

public sealed class S3ObjectStore : IObjectStore
{
    private readonly IAmazonS3 client;
    private readonly string bucket;

    public string Description { get; }

    public S3ObjectStore(StorageOptions options)
    {
        if (options.TimeoutSeconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options));
        }

        var configuration = new AmazonS3Config
        {
            ServiceURL = options.ServiceUrl,
            AuthenticationRegion = options.Region,
            ForcePathStyle = true,
            Timeout = TimeSpan.FromSeconds(options.TimeoutSeconds),
            MaxErrorRetry = 2,
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED
        };

        client = new AmazonS3Client(new EnvironmentVariablesAWSCredentials(), configuration);
        bucket = options.Bucket;
        Description = "s3:" + options.ServiceUrl + "/" + bucket;
    }

    public async Task PutFileAsync(
        string key,
        string sourceFile,
        CancellationToken cancellationToken)
    {
        FileOperations.ValidateObjectKey(key);

        await using var stream = FileOperations.OpenRead(sourceFile);

        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            InputStream = stream,
            AutoCloseStream = false,
            AutoResetStreamPosition = true,
            UseChunkEncoding = false,
            ContentType = "application/octet-stream"
        };

        request.Headers.ContentLength = stream.Length;

        await client.PutObjectAsync(request, cancellationToken);
    }

    public async Task GetFileAsync(
        string key,
        string destinationFile,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        FileOperations.ValidateObjectKey(key);

        string temporary = destinationFile + "." + Guid.NewGuid().ToString("N") + ".tmp";
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(destinationFile))!);

        try
        {
            using var response = await client.GetObjectAsync(bucket, key, cancellationToken);

            if (response.ContentLength < 0
                || response.ContentLength > maximumBytes)
            {
                throw new InvalidDataException("S3 object exceeds its permitted length.");
            }

            await using (var output = new FileStream(
                temporary,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                FileOperations.BufferBytes,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                var buffer = new byte[FileOperations.BufferBytes];
                long total = 0;
                int count;

                while ((count = await response.ResponseStream.ReadAsync(buffer, cancellationToken)) != 0)
                {
                    total = checked(total + count);

                    if (total > maximumBytes)
                    {
                        throw new InvalidDataException("S3 response exceeded the requested bound.");
                    }

                    await output.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
                }

                if (total != response.ContentLength)
                {
                    throw new EndOfStreamException("Incomplete S3 object.");
                }

                await output.FlushAsync(cancellationToken);
                output.Flush(flushToDisk: true);
            }

            File.Move(temporary, destinationFile, overwrite: true);
        }
        finally
        {
            FileOperations.DeleteIfExists(temporary);
        }
    }

    public async Task DeleteAsync(string key, CancellationToken cancellationToken)
        => await client.DeleteObjectAsync(bucket, FileOperations.ValidateObjectKey(key), cancellationToken);

    public ValueTask DisposeAsync()
    {
        client.Dispose();

        return ValueTask.CompletedTask;
    }
}