using System.Security.Claims;
using Microsoft.AspNetCore.Http.HttpResults;

internal static class DevicesEndpoints
{
    public static IEndpointRouteBuilder MapDevicesEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/devices", GetDevices).RequireAuthorization();
        app.MapGet("/device", GetDeviceById).RequireAuthorization();
        app.MapGet("/groups", GetGroups).RequireAuthorization();
        app.MapGet("/device-anomalies", GetDeviceAnomalies).RequireAuthorization();
        app.MapGet("/device-state-latest", GetDeviceStateLatest).RequireAuthorization();
        app.MapGet("/device-timeseries-hourly", GetDeviceTimeseriesHourly).RequireAuthorization();
        app.MapGet("/device-trends-daily", GetDeviceTrendsDaily).RequireAuthorization();
        app.MapGet("/device-out-of-range-events", GetOutOfRangeEvents).RequireAuthorization();
        return app;
    }

    private static async Task<Results<Ok<IReadOnlyList<DeviceDto>>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDevices(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DevicesEndpoint");
        string userId;
        try
        {
            userId = user.GetVerifiedUserId();
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
            return TypedResults.Unauthorized();
        }

        var groupId = context.Request.Query["groupId"].ToString();

        try
        {
            var devices = await devicesQueryService.GetDevicesForUserAsync(
                userId,
                string.IsNullOrWhiteSpace(groupId) ? null : groupId,
                context.RequestAborted);
            return TypedResults.Ok(devices);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid devices table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery devices configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query devices.");
            return TypedResults.Problem(
                title: "Devices query failed",
                detail: "Unable to fetch devices at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<DeviceDto>, NotFound<ErrorResponse>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDeviceById(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DeviceEndpoint");
        string userId;
        try
        {
            userId = user.GetVerifiedUserId();
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
            return TypedResults.Unauthorized();
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        try
        {
            var device = await devicesQueryService.GetDeviceForUserAsync(userId, deviceId, context.RequestAborted);
            if (device is null)
            {
                return TypedResults.NotFound(new ErrorResponse("Device not found for this user."));
            }

            return TypedResults.Ok(device);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid devices table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery devices configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query device details.");
            return TypedResults.Problem(
                title: "Device query failed",
                detail: "Unable to fetch device at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<IReadOnlyList<GroupDto>>, ProblemHttpResult, UnauthorizedHttpResult>> GetGroups(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("GroupsEndpoint");
        string userId;
        try
        {
            userId = user.GetVerifiedUserId();
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
            return TypedResults.Unauthorized();
        }

        try
        {
            var groups = await devicesQueryService.GetGroupsForUserAsync(userId, context.RequestAborted);
            return TypedResults.Ok(groups);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid groups table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery groups configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query groups.");
            return TypedResults.Problem(
                title: "Groups query failed",
                detail: "Unable to fetch groups at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<IReadOnlyList<DeviceAnomalyDto>>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDeviceAnomalies(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DeviceAnomaliesEndpoint");
        string userId;
        try
        {
            userId = user.GetVerifiedUserId();
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
            return TypedResults.Unauthorized();
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        try
        {
            var anomalies = await devicesQueryService.GetDeviceAnomaliesForUserAsync(userId, deviceId, context.RequestAborted);
            return TypedResults.Ok(anomalies);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid anomalies table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery anomalies configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query device anomalies.");
            return TypedResults.Problem(
                title: "Device anomalies query failed",
                detail: "Unable to fetch device anomalies at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<DeviceStateLatestDto>, NotFound<ErrorResponse>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDeviceStateLatest(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DeviceStateLatestEndpoint");
        if (!TryGetAuthenticatedUserId(user, logger, out var userId, out var unauthorized))
        {
            return unauthorized!;
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        try
        {
            var latest = await devicesQueryService.GetDeviceStateLatestForUserAsync(userId!, deviceId, context.RequestAborted);
            return latest is null
                ? TypedResults.NotFound(new ErrorResponse("No state found for this device and user."))
                : TypedResults.Ok(latest);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid state latest table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery device state configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query latest device state.");
            return TypedResults.Problem(
                title: "Device state query failed",
                detail: "Unable to fetch latest device state at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<DeviceTimeseriesResponseDto>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDeviceTimeseriesHourly(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DeviceTimeseriesHourlyEndpoint");
        if (!TryGetAuthenticatedUserId(user, logger, out var userId, out var unauthorized))
        {
            return unauthorized!;
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        if (!TryParseBoundedInt(context.Request.Query["limit"], 168, 1, 5000, out var limit))
        {
            return TypedResults.BadRequest(new ErrorResponse("'limit' must be an integer between 1 and 5000."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["from"], out var from))
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' must be a valid ISO-8601 timestamp."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["to"], out var to))
        {
            return TypedResults.BadRequest(new ErrorResponse("'to' must be a valid ISO-8601 timestamp."));
        }

        if (from.HasValue && to.HasValue && from.Value > to.Value)
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' cannot be later than 'to'."));
        }

        try
        {
            var response = await devicesQueryService.GetDeviceTimeseriesHourlyForUserAsync(
                userId!,
                deviceId,
                limit,
                from,
                to,
                context.RequestAborted);
            return TypedResults.Ok(response);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid timeseries table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery timeseries configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query device timeseries.");
            return TypedResults.Problem(
                title: "Device timeseries query failed",
                detail: "Unable to fetch device timeseries at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<DeviceTrendsResponseDto>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetDeviceTrendsDaily(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("DeviceTrendsDailyEndpoint");
        if (!TryGetAuthenticatedUserId(user, logger, out var userId, out var unauthorized))
        {
            return unauthorized!;
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        if (!TryParseBoundedInt(context.Request.Query["limit"], 90, 1, 3650, out var limit))
        {
            return TypedResults.BadRequest(new ErrorResponse("'limit' must be an integer between 1 and 3650."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["from"], out var from))
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' must be a valid ISO-8601 timestamp."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["to"], out var to))
        {
            return TypedResults.BadRequest(new ErrorResponse("'to' must be a valid ISO-8601 timestamp."));
        }

        if (from.HasValue && to.HasValue && from.Value > to.Value)
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' cannot be later than 'to'."));
        }

        try
        {
            var response = await devicesQueryService.GetDeviceTrendsDailyForUserAsync(
                userId!,
                deviceId,
                limit,
                from,
                to,
                context.RequestAborted);
            return TypedResults.Ok(response);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid trends table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery trends configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query device trends.");
            return TypedResults.Problem(
                title: "Device trends query failed",
                detail: "Unable to fetch device trends at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<Results<Ok<IReadOnlyList<OutOfRangeEventDto>>, BadRequest<ErrorResponse>, ProblemHttpResult, UnauthorizedHttpResult>> GetOutOfRangeEvents(
        HttpContext context,
        ClaimsPrincipal user,
        DevicesQueryService devicesQueryService,
        ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger("OutOfRangeEventsEndpoint");
        if (!TryGetAuthenticatedUserId(user, logger, out var userId, out var unauthorized))
        {
            return unauthorized!;
        }

        var deviceId = context.Request.Query["deviceId"].ToString();
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return TypedResults.BadRequest(new ErrorResponse("'deviceId' query parameter is required."));
        }

        if (!TryParseBoundedInt(context.Request.Query["limit"], 200, 1, 5000, out var limit))
        {
            return TypedResults.BadRequest(new ErrorResponse("'limit' must be an integer between 1 and 5000."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["from"], out var from))
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' must be a valid ISO-8601 timestamp."));
        }

        if (!TryParseOptionalInstant(context.Request.Query["to"], out var to))
        {
            return TypedResults.BadRequest(new ErrorResponse("'to' must be a valid ISO-8601 timestamp."));
        }

        if (from.HasValue && to.HasValue && from.Value > to.Value)
        {
            return TypedResults.BadRequest(new ErrorResponse("'from' cannot be later than 'to'."));
        }

        try
        {
            var response = await devicesQueryService.GetOutOfRangeEventsForUserAsync(
                userId!,
                deviceId,
                limit,
                from,
                to,
                context.RequestAborted);
            return TypedResults.Ok(response);
        }
        catch (InvalidOperationException ex)
        {
            logger.LogError(ex, "Invalid out-of-range events table configuration.");
            return TypedResults.Problem(
                title: "Configuration error",
                detail: "Server BigQuery out-of-range configuration is invalid.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to query out-of-range events.");
            return TypedResults.Problem(
                title: "Out-of-range events query failed",
                detail: "Unable to fetch out-of-range events at this time.",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static bool TryGetAuthenticatedUserId(
        ClaimsPrincipal user,
        ILogger logger,
        out string? userId,
        out UnauthorizedHttpResult? unauthorized)
    {
        try
        {
            userId = user.GetVerifiedUserId();
            unauthorized = null;
            return true;
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning(ex, "Verified uid is missing from authenticated principal.");
            userId = null;
            unauthorized = TypedResults.Unauthorized();
            return false;
        }
    }

    private static bool TryParseBoundedInt(string? raw, int defaultValue, int min, int max, out int value)
    {
        value = defaultValue;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!int.TryParse(raw, out var parsed))
        {
            return false;
        }

        if (parsed < min || parsed > max)
        {
            return false;
        }

        value = parsed;
        return true;
    }

    private static bool TryParseOptionalInstant(string? raw, out DateTimeOffset? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!DateTimeOffset.TryParse(raw, out var parsed))
        {
            return false;
        }

        value = parsed.ToUniversalTime();
        return true;
    }
}
