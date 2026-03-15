using System.Globalization;
using System.Text.RegularExpressions;
using Google.Cloud.BigQuery.V2;

internal sealed class DevicesQueryService
{
    private static string ProjectId => GetRequiredEnv("BQ_PROJECT_ID");

    public async Task<IReadOnlyList<DeviceDto>> GetDevicesForUserAsync(string userId, string? groupId, CancellationToken cancellationToken)
    {
        var config = GetDevicesTableConfig();
        var qualifiedTable = QualifyTable(config);

        var query = $"""
                     SELECT
                       device_id,
                       device_name,
                       user_id,
                       group_id,
                       plant_type,
                       soil_type,
                       location,
                       location_notes,
                       operational_status,
                       firmware_version,
                       last_sync,
                       configured_at,
                       created_at,
                       updated_at
                     FROM {qualifiedTable}
                     WHERE user_id = @userId
                     """;

        var parameters = new List<BigQueryParameter>
        {
            new("userId", BigQueryDbType.String, userId)
        };

        if (!string.IsNullOrWhiteSpace(groupId))
        {
            query += "\nAND group_id = @groupId";
            parameters.Add(new BigQueryParameter("groupId", BigQueryDbType.String, groupId.Trim()));
        }

        query += "\nORDER BY created_at DESC NULLS LAST";

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, parameters, cancellationToken: cancellationToken);
        return rows.Select(MapDevice).ToList();
    }

    public async Task<DeviceDto?> GetDeviceForUserAsync(string userId, string deviceId, CancellationToken cancellationToken)
    {
        var config = GetDevicesTableConfig();
        var qualifiedTable = QualifyTable(config);

        var query = $"""
                     SELECT
                       device_id,
                       device_name,
                       user_id,
                       group_id,
                       plant_type,
                       soil_type,
                       location,
                       location_notes,
                       operational_status,
                       firmware_version,
                       last_sync,
                       configured_at,
                       created_at,
                       updated_at
                     FROM {qualifiedTable}
                     WHERE user_id = @userId
                       AND device_id = @deviceId
                     LIMIT 1
                     """;

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, new[]
        {
            new BigQueryParameter("userId", BigQueryDbType.String, userId),
            new BigQueryParameter("deviceId", BigQueryDbType.String, deviceId)
        }, cancellationToken: cancellationToken);

        return rows.Select(MapDevice).FirstOrDefault();
    }

    public async Task<IReadOnlyList<GroupDto>> GetGroupsForUserAsync(string userId, CancellationToken cancellationToken)
    {
        var config = GetGroupsTableConfig();
        var qualifiedTable = QualifyTable(config);

        var query = $"""
                     SELECT
                       group_id,
                       group_name,
                       notes,
                       created_at,
                       updated_at,
                       location,
                       user_id
                     FROM {qualifiedTable}
                     WHERE user_id = @userId
                     ORDER BY created_at DESC NULLS LAST
                     """;

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, new[]
        {
            new BigQueryParameter("userId", BigQueryDbType.String, userId)
        }, cancellationToken: cancellationToken);

        return rows.Select(MapGroup).ToList();
    }

    public async Task<IReadOnlyList<DeviceAnomalyDto>> GetDeviceAnomaliesForUserAsync(
        string userId,
        string deviceId,
        CancellationToken cancellationToken)
    {
        var anomaliesConfig = GetAnomaliesTableConfig();
        var devicesConfig = GetDevicesTableConfig();
        var anomaliesTable = QualifyTable(anomaliesConfig);
        var devicesTable = QualifyTable(devicesConfig);

        var query = $"""
                     SELECT
                       a.device_id,
                       a.reading_time,
                       a.metric,
                       a.value,
                       a.method,
                       a.score,
                       a.severity,
                       a.explanation,
                       a.created_at
                     FROM {anomaliesTable} a
                     WHERE a.device_id = @deviceId
                       AND EXISTS (
                         SELECT 1
                         FROM {devicesTable} d
                         WHERE d.device_id = a.device_id
                           AND d.user_id = @userId
                       )
                     ORDER BY a.reading_time DESC
                     """;

        var client = BigQueryClient.Create(anomaliesConfig.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, new[]
        {
            new BigQueryParameter("deviceId", BigQueryDbType.String, deviceId),
            new BigQueryParameter("userId", BigQueryDbType.String, userId)
        }, cancellationToken: cancellationToken);

        return rows.Select(MapAnomaly).ToList();
    }

    private static TableConfig GetDevicesTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_DEVICES_DATASET");
        var table = GetRequiredEnv("BQ_DEVICES_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "devices");
    }

    private static TableConfig GetGroupsTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_GROUPS_DATASET");
        var table = GetRequiredEnv("BQ_GROUPS_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "groups");
    }

    private static TableConfig GetAnomaliesTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_ANOMALIES_DATASET");
        var table = GetRequiredEnv("BQ_ANOMALIES_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "anomalies");
    }

    private static string GetRequiredEnv(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        throw new InvalidOperationException($"Missing required environment variable: {name}");
    }

    private static TableConfig ValidateAndBuild(string projectId, string dataset, string table, string configName)
    {
        if (!IsValidProjectId(projectId) || !IsValidIdentifier(dataset) || !IsValidIdentifier(table))
        {
            throw new InvalidOperationException(
                $"Invalid BigQuery {configName} table configuration. project={projectId}, dataset={dataset}, table={table}");
        }

        return new TableConfig(projectId, dataset, table);
    }

    private static string QualifyTable(TableConfig config) => $"`{config.ProjectId}.{config.Dataset}.{config.Table}`";

    private static bool IsValidProjectId(string value) =>
        Regex.IsMatch(value, @"^[a-z][a-z0-9\-]{4,29}$", RegexOptions.CultureInvariant);

    private static bool IsValidIdentifier(string value) =>
        Regex.IsMatch(value, @"^[A-Za-z0-9_]+$", RegexOptions.CultureInvariant);

    private static DeviceDto MapDevice(BigQueryRow row)
    {
        return new DeviceDto(
            DeviceId: ToNullableString(row["device_id"]) ?? string.Empty,
            DeviceName: ToNullableString(row["device_name"]),
            UserId: ToNullableString(row["user_id"]) ?? string.Empty,
            GroupId: ToNullableString(row["group_id"]),
            PlantType: ToNullableInt(row["plant_type"]),
            SoilType: ToNullableInt(row["soil_type"]),
            Location: ToNullableString(row["location"]),
            LocationNotes: ToNullableString(row["location_notes"]),
            OperationalStatus: ToNullableInt(row["operational_status"]),
            FirmwareVersion: ToNullableString(row["firmware_version"]),
            LastSync: ToIso8601Nullable(row["last_sync"]),
            ConfiguredAt: ToIso8601Nullable(row["configured_at"]),
            CreatedAt: ToIso8601Nullable(row["created_at"]),
            UpdatedAt: ToIso8601Nullable(row["updated_at"]));
    }

    private static GroupDto MapGroup(BigQueryRow row)
    {
        return new GroupDto(
            GroupId: ToNullableString(row["group_id"]) ?? string.Empty,
            GroupName: ToNullableString(row["group_name"]),
            Notes: ToNullableString(row["notes"]),
            CreatedAt: ToIso8601Nullable(row["created_at"]),
            UpdatedAt: ToIso8601Nullable(row["updated_at"]),
            Location: ToNullableString(row["location"]),
            UserId: ToNullableString(row["user_id"]) ?? string.Empty);
    }

    private static DeviceAnomalyDto MapAnomaly(BigQueryRow row)
    {
        return new DeviceAnomalyDto(
            DeviceId: ToNullableString(row["device_id"]) ?? string.Empty,
            ReadingTime: ToIso8601Nullable(row["reading_time"]) ?? string.Empty,
            Metric: ToNullableString(row["metric"]) ?? string.Empty,
            Value: ToNullableDouble(row["value"]),
            Method: ToNullableString(row["method"]) ?? string.Empty,
            Score: ToNullableDouble(row["score"]),
            Severity: ToNullableString(row["severity"]) ?? string.Empty,
            Explanation: ToNullableString(row["explanation"]),
            CreatedAt: ToIso8601Nullable(row["created_at"]) ?? string.Empty);
    }

    private static string? ToNullableString(object? value)
    {
        return value switch
        {
            null => null,
            string s => s,
            _ => value.ToString()
        };
    }

    private static int? ToNullableInt(object? value)
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

    private static double? ToNullableDouble(object? value)
    {
        return value switch
        {
            null => null,
            double d => d,
            float f => f,
            decimal d => (double)d,
            string s when double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => null
        };
    }

    private static string? ToIso8601Nullable(object? value)
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

    private sealed record TableConfig(string ProjectId, string Dataset, string Table);
}
