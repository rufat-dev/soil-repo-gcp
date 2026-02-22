using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Google.Cloud.BigQuery.V2;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();

var allowedOrigins = (Environment.GetEnvironmentVariable("ALLOWED_ORIGINS") ?? string.Empty)
    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

const string corsPolicyName = "ConfiguredOrigins";
var corsEnabled = false;
if (allowedOrigins.Length > 0)
{
    builder.Services.AddCors(options =>
    {
        options.AddPolicy(corsPolicyName, policy =>
        {
            if (allowedOrigins.Length == 1 && allowedOrigins[0] == "*")
            {
                policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
            }
            else
            {
                policy.WithOrigins(allowedOrigins).AllowAnyMethod().AllowAnyHeader();
            }
        });
    });
    corsEnabled = true;
}

var app = builder.Build();

if (corsEnabled)
{
    app.UseCors(corsPolicyName);
}

app.MapGet("/", () => Results.Ok("OK"));

app.MapGet("/users", async (HttpContext context, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("UsersEndpoint");
    var cancellationToken = context.RequestAborted;

    if (!TryParseBoundedInt(context.Request.Query["limit"], defaultValue: 200, min: 1, max: 1000, out var limit, out var limitError))
    {
        return Results.BadRequest(new ErrorResponse(limitError));
    }

    if (!TryParseBoundedInt(context.Request.Query["offset"], defaultValue: 0, min: 0, max: 100000, out var offset, out var offsetError))
    {
        return Results.BadRequest(new ErrorResponse(offsetError));
    }

    var projectId = Environment.GetEnvironmentVariable("BQ_PROJECT_ID") ?? "soil-report-486813";
    var dataset = Environment.GetEnvironmentVariable("BQ_DATASET") ?? "crm";
    var table = Environment.GetEnvironmentVariable("BQ_TABLE") ?? "users";

    if (!IsValidProjectId(projectId) || !IsValidIdentifier(dataset) || !IsValidIdentifier(table))
    {
        logger.LogError("Invalid BigQuery identifier configuration. project={Project}, dataset={Dataset}, table={Table}", projectId, dataset, table);
        return Results.Problem(
            title: "Configuration error",
            detail: "Server BigQuery configuration is invalid.",
            statusCode: StatusCodes.Status500InternalServerError);
    }

    var qualifiedTable = $"`{projectId}.{dataset}.{table}`";
    var query = $"""
                 SELECT user_id, email, phone_number, full_name, role, created_at, updated_at
                 FROM {qualifiedTable}
                 ORDER BY created_at DESC NULLS LAST
                 LIMIT {limit} OFFSET {offset}
                 """;

    try
    {
        var client = await BigQueryClient.CreateAsync(projectId, cancellationToken: cancellationToken);
        var rows = await client.ExecuteQueryAsync(query, parameters: null, options: null, cancellationToken: cancellationToken);

        var users = rows.Select(MapUser).ToList();
        return Results.Ok(users);
    }
    catch (OperationCanceledException)
    {
        logger.LogInformation("Request cancelled while querying BigQuery.");
        return Results.StatusCode(StatusCodes.Status499ClientClosedRequest);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to query BigQuery users.");
        return Results.Problem(
            title: "BigQuery query failed",
            detail: "Unable to fetch users at this time.",
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.Run();

static bool TryParseBoundedInt(string? rawValue, int defaultValue, int min, int max, out int value, out string? error)
{
    value = defaultValue;
    error = null;

    if (string.IsNullOrWhiteSpace(rawValue))
    {
        return true;
    }

    if (!int.TryParse(rawValue, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
    {
        error = $"Invalid integer value: '{rawValue}'.";
        return false;
    }

    if (parsed < min || parsed > max)
    {
        error = $"Value '{rawValue}' must be between {min} and {max}.";
        return false;
    }

    value = parsed;
    return true;
}

static bool IsValidProjectId(string value) =>
    Regex.IsMatch(value, @"^[a-z][a-z0-9\-]{4,29}$", RegexOptions.CultureInvariant);

static bool IsValidIdentifier(string value) =>
    Regex.IsMatch(value, @"^[A-Za-z0-9_]+$", RegexOptions.CultureInvariant);

static UserDto MapUser(BigQueryRow row)
{
    return new UserDto(
        UserId: ToNullableString(row["user_id"]) ?? string.Empty,
        Email: ToNullableString(row["email"]) ?? string.Empty,
        PhoneNumber: ToNullableString(row["phone_number"]),
        FullName: ToNullableString(row["full_name"]),
        Role: ToNullableInt(row["role"]),
        CreatedAt: ToIso8601Nullable(row["created_at"]),
        UpdatedAt: ToIso8601Nullable(row["updated_at"]));
}

static string? ToNullableString(object? value)
{
    return value switch
    {
        null => null,
        string s => s,
        _ => value.ToString()
    };
}

static int? ToNullableInt(object? value)
{
    return value switch
    {
        null => null,
        int i => i,
        long l when l is >= int.MinValue and <= int.MaxValue => (int)l,
        decimal d when d is >= int.MinValue and <= int.MaxValue => (int)d,
        string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
        _ => null
    };
}

static string? ToIso8601Nullable(object? value)
{
    return value switch
    {
        null => null,
        DateTime dt => dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        string s when DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
            => parsed.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        _ => value.ToString()
    };
}

internal sealed record UserDto(
    [property: JsonPropertyName("user_id")] string UserId,
    [property: JsonPropertyName("email")] string Email,
    [property: JsonPropertyName("phone_number")] string? PhoneNumber,
    [property: JsonPropertyName("full_name")] string? FullName,
    [property: JsonPropertyName("role")] int? Role,
    [property: JsonPropertyName("created_at")] string? CreatedAt,
    [property: JsonPropertyName("updated_at")] string? UpdatedAt);

internal sealed record ErrorResponse([property: JsonPropertyName("error")] string? Error);
