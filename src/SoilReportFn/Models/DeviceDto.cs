using System.Text.Json.Serialization;

internal sealed record DeviceDto(
    [property: JsonPropertyName("device_id")] string DeviceId,
    [property: JsonPropertyName("device_name")] string? DeviceName,
    [property: JsonPropertyName("user_id")] string UserId,
    [property: JsonPropertyName("group_id")] string? GroupId,
    [property: JsonPropertyName("plant_type")] int? PlantType,
    [property: JsonPropertyName("soil_type")] int? SoilType,
    [property: JsonPropertyName("location")] string? Location,
    [property: JsonPropertyName("location_notes")] string? LocationNotes,
    [property: JsonPropertyName("operational_status")] int? OperationalStatus,
    [property: JsonPropertyName("firmware_version")] string? FirmwareVersion,
    [property: JsonPropertyName("last_sync")] string? LastSync,
    [property: JsonPropertyName("configured_at")] string? ConfiguredAt,
    [property: JsonPropertyName("created_at")] string? CreatedAt,
    [property: JsonPropertyName("updated_at")] string? UpdatedAt);

internal sealed record GroupDto(
    [property: JsonPropertyName("group_id")] string GroupId,
    [property: JsonPropertyName("group_name")] string? GroupName,
    [property: JsonPropertyName("notes")] string? Notes,
    [property: JsonPropertyName("created_at")] string? CreatedAt,
    [property: JsonPropertyName("updated_at")] string? UpdatedAt,
    [property: JsonPropertyName("location")] string? Location,
    [property: JsonPropertyName("user_id")] string UserId);

internal sealed record DeviceAnomalyDto(
    [property: JsonPropertyName("device_id")] string DeviceId,
    [property: JsonPropertyName("reading_time")] string ReadingTime,
    [property: JsonPropertyName("metric")] string Metric,
    [property: JsonPropertyName("value")] double? Value,
    [property: JsonPropertyName("method")] string Method,
    [property: JsonPropertyName("score")] double? Score,
    [property: JsonPropertyName("severity")] string Severity,
    [property: JsonPropertyName("explanation")] string? Explanation,
    [property: JsonPropertyName("created_at")] string CreatedAt);
