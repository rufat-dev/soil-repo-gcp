using System.Security.Claims;
using Microsoft.AspNetCore.Http.HttpResults;

internal static class DevicesEndpoints
{
    public static IEndpointRouteBuilder MapDevicesEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/devices", GetDevices).RequireAuthorization();
        app.MapGet("/device", GetDeviceById).RequireAuthorization();
        app.MapGet("/groups", GetGroups).RequireAuthorization();
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
}
