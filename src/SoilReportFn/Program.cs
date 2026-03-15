using System.Globalization;
using System.Security.Claims;
using System.Text.Json.Serialization;
using System.Text.Encodings.Web;
using System.Text.RegularExpressions;
using FirebaseAdmin;
using FirebaseAdmin.Auth;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();

var firebaseProjectId = Environment.GetEnvironmentVariable("FIREBASE_PROJECT_ID")
    ?? Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT")
    ?? Environment.GetEnvironmentVariable("GCP_PROJECT")
    ?? "soil-report-486813";

builder.Services.AddSingleton(_ =>
{
    if (FirebaseApp.DefaultInstance is not null)
    {
        return FirebaseApp.DefaultInstance;
    }

    return FirebaseApp.Create(new AppOptions
    {
        Credential = GoogleCredential.GetApplicationDefault(),
        ProjectId = firebaseProjectId
    });
});
builder.Services.AddSingleton(sp => FirebaseAuth.GetAuth(sp.GetRequiredService<FirebaseApp>()));

builder.Services
    .AddAuthentication(options =>
    {
        options.DefaultAuthenticateScheme = FirebaseAuthenticationHandler.SchemeName;
        options.DefaultChallengeScheme = FirebaseAuthenticationHandler.SchemeName;
    })
    .AddScheme<AuthenticationSchemeOptions, FirebaseAuthenticationHandler>(
        FirebaseAuthenticationHandler.SchemeName,
        _ => { });
builder.Services.AddAuthorization();

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

app.UseAuthentication();
app.UseAuthorization();

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

    var (projectId, dataset, table) = GetBigQueryIdentifiers();

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
        var client = BigQueryClient.Create(projectId);
        var rows = await client.ExecuteQueryAsync(query, parameters: null);

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
}).RequireAuthorization();

app.MapPost("/auth/bootstrap", async (HttpContext context, ClaimsPrincipal user, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("AuthBootstrapEndpoint");

    BootstrapRequest? request;
    try
    {
        request = await context.Request.ReadFromJsonAsync<BootstrapRequest>(context.RequestAborted);
    }
    catch (Exception ex)
    {
        logger.LogWarning(ex, "Invalid bootstrap request body.");
        return Results.BadRequest(new ErrorResponse("Request body must be valid JSON."));
    }

    if (request is null)
    {
        return Results.BadRequest(new ErrorResponse("Request body is required."));
    }

    string userId;
    try
    {
        userId = user.GetVerifiedUserId();
    }
    catch (InvalidOperationException ex)
    {
        logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
        return Results.Unauthorized();
    }

    var email = user.FindFirstValue(ClaimTypes.Email) ?? user.FindFirstValue("email");
    var fullName = request.FullName?.Trim();
    var phoneNumber = request.PhoneNumber?.Trim();
    var role = 0;

    var (projectId, dataset, table) = GetBigQueryIdentifiers();
    if (!IsValidProjectId(projectId) || !IsValidIdentifier(dataset) || !IsValidIdentifier(table))
    {
        logger.LogError("Invalid BigQuery identifier configuration. project={Project}, dataset={Dataset}, table={Table}", projectId, dataset, table);
        return Results.Problem(
            title: "Configuration error",
            detail: "Server BigQuery configuration is invalid.",
            statusCode: StatusCodes.Status500InternalServerError);
    }

    var qualifiedTable = $"`{projectId}.{dataset}.{table}`";

    try
    {
        var client = BigQueryClient.Create(projectId);

        var existenceQuery = $"""
                              SELECT 1
                              FROM {qualifiedTable}
                              WHERE user_id = @userId
                              LIMIT 1
                              """;
        var existenceRows = await client.ExecuteQueryAsync(existenceQuery, new[]
        {
            new BigQueryParameter("userId", BigQueryDbType.String, userId)
        });
        var isNewUser = !existenceRows.Any();

        var mergeQuery = $"""
                          MERGE {qualifiedTable} AS target
                          USING (
                            SELECT @userId AS user_id, @email AS email, @phoneNumber AS phone_number, @fullName AS full_name, @role AS role
                          ) AS source
                          ON target.user_id = source.user_id
                          WHEN MATCHED THEN
                            UPDATE SET
                              email = COALESCE(source.email, target.email),
                              phone_number = COALESCE(source.phone_number, target.phone_number),
                              full_name = COALESCE(source.full_name, target.full_name),
                              updated_at = CURRENT_TIMESTAMP()
                          WHEN NOT MATCHED THEN
                            INSERT (user_id, email, phone_number, full_name, role, created_at, updated_at)
                            VALUES (source.user_id, source.email, source.phone_number, source.full_name, source.role, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                          """;
        await client.ExecuteQueryAsync(mergeQuery, new[]
        {
            new BigQueryParameter("userId", BigQueryDbType.String, userId),
            new BigQueryParameter("email", BigQueryDbType.String, email),
            new BigQueryParameter("phoneNumber", BigQueryDbType.String, phoneNumber),
            new BigQueryParameter("fullName", BigQueryDbType.String, fullName),
            new BigQueryParameter("role", BigQueryDbType.Int64, role)
        });

        var fetchQuery = $"""
                         SELECT user_id, email, phone_number, full_name, role, created_at, updated_at
                         FROM {qualifiedTable}
                         WHERE user_id = @userId
                         LIMIT 1
                         """;
        var profileRows = await client.ExecuteQueryAsync(fetchQuery, new[]
        {
            new BigQueryParameter("userId", BigQueryDbType.String, userId)
        });

        var appUser = profileRows.Select(MapUser).FirstOrDefault()
                      ?? new UserDto(
                          UserId: userId,
                          Email: email ?? string.Empty,
                          PhoneNumber: phoneNumber,
                          FullName: fullName,
                          Role: 0,
                          CreatedAt: null,
                          UpdatedAt: null);

        return Results.Ok(new BootstrapResponse(isNewUser, appUser));
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to bootstrap user profile.");
        return Results.Problem(
            title: "Bootstrap failed",
            detail: "Unable to initialize user profile.",
            statusCode: StatusCodes.Status500InternalServerError);
    }
}).RequireAuthorization();

app.Run();

static (string ProjectId, string Dataset, string Table) GetBigQueryIdentifiers()
{
    var projectId = Environment.GetEnvironmentVariable("BQ_PROJECT_ID") ?? "soil-report-486813";
    var dataset = Environment.GetEnvironmentVariable("BQ_DATASET") ?? "crm";
    var table = Environment.GetEnvironmentVariable("BQ_TABLE") ?? "users";
    return (projectId, dataset, table);
}

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

internal sealed class FirebaseAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    FirebaseAuth firebaseAuth)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    internal const string SchemeName = "FirebaseBearer";
    private readonly FirebaseAuth _firebaseAuth = firebaseAuth;

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue("Authorization", out var authorizationValues))
        {
            return AuthenticateResult.NoResult();
        }

        var authorization = authorizationValues.ToString();
        if (!authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            return AuthenticateResult.NoResult();
        }

        var token = authorization["Bearer ".Length..].Trim();
        if (string.IsNullOrWhiteSpace(token))
        {
            return AuthenticateResult.Fail("Missing bearer token.");
        }

        try
        {
            var decodedToken = await _firebaseAuth.VerifyIdTokenAsync(token);

            var claims = new List<Claim>
            {
                new(ClaimTypes.NameIdentifier, decodedToken.Uid),
                new("uid", decodedToken.Uid)
            };

            if (TryGetStringClaim(decodedToken.Claims, "email", out var email))
            {
                claims.Add(new Claim(ClaimTypes.Email, email));
                claims.Add(new Claim("email", email));
            }

            if (TryGetStringClaim(decodedToken.Claims, "name", out var name))
            {
                claims.Add(new Claim("name", name));
            }

            var identity = new ClaimsIdentity(claims, SchemeName);
            var principal = new ClaimsPrincipal(identity);
            var ticket = new AuthenticationTicket(principal, SchemeName);
            return AuthenticateResult.Success(ticket);
        }
        catch (FirebaseAuthException ex)
        {
            Logger.LogWarning(ex, "Firebase token verification failed.");
            return AuthenticateResult.Fail("Invalid Firebase token.");
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Unexpected error while verifying Firebase token.");
            return AuthenticateResult.Fail("Token verification failed.");
        }
    }

    private static bool TryGetStringClaim(IReadOnlyDictionary<string, object> claims, string key, out string value)
    {
        value = string.Empty;
        if (!claims.TryGetValue(key, out var rawValue) || rawValue is null)
        {
            return false;
        }

        value = rawValue.ToString() ?? string.Empty;
        return value.Length > 0;
    }
}

internal static class ClaimsPrincipalExtensions
{
    public static string GetVerifiedUserId(this ClaimsPrincipal principal)
    {
        var uid = principal.FindFirstValue("uid") ?? principal.FindFirstValue(ClaimTypes.NameIdentifier);
        if (!string.IsNullOrWhiteSpace(uid))
        {
            return uid;
        }

        throw new InvalidOperationException("Verified user id claim is missing.");
    }
}

internal sealed record BootstrapResponse(
    [property: JsonPropertyName("is_new_user")] bool IsNewUser,
    [property: JsonPropertyName("user")] UserDto User);

internal sealed class BootstrapRequest
{
    [JsonPropertyName("fullName")] public string? FullName { get; init; }
    [JsonPropertyName("phoneNumber")] public string? PhoneNumber { get; init; }
    [JsonPropertyName("role")] public int? Role { get; init; }

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
