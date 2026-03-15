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
        await EnsureDeviceOwnedByUserAsync(userId, deviceId, cancellationToken);

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

    public async Task<DeviceStateLatestDto?> GetDeviceStateLatestForUserAsync(
        string userId,
        string deviceId,
        CancellationToken cancellationToken)
    {
        await EnsureDeviceOwnedByUserAsync(userId, deviceId, cancellationToken);

        var config = GetStateLatestTableConfig();
        var table = QualifyTable(config);
        var ownershipFilter = BuildOwnershipFilterSql(GetDevicesTableConfig(), "s.device_id");

        var query = $"""
                     SELECT
                       s.device_id,
                       s.reading_time,
                       s.temperature,
                       s.moisture,
                       s.conductivity,
                       s.ph_value,
                       s.npk_content,
                       s.temperature_low,
                       s.temperature_high,
                       s.moisture_low,
                       s.moisture_high,
                       s.conductivity_low,
                       s.conductivity_high,
                       s.ph_low,
                       s.ph_high,
                       s.risk_score,
                       s.ai_status,
                       s.temperature_trend,
                       s.moisture_trend,
                       s.conductivity_trend,
                       s.ph_trend,
                       s.last_seen_minutes,
                       s.updated_at
                     FROM {table} s
                     WHERE s.device_id = @deviceId
                       AND {ownershipFilter}
                     ORDER BY s.updated_at DESC
                     LIMIT 1
                     """;

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, BuildOwnershipParameters(userId, deviceId), cancellationToken: cancellationToken);
        return rows.Select(MapStateLatest).FirstOrDefault();
    }

    public async Task<DeviceTimeseriesResponseDto> GetDeviceTimeseriesHourlyForUserAsync(
        string userId,
        string deviceId,
        int limit,
        DateTimeOffset? from,
        DateTimeOffset? to,
        CancellationToken cancellationToken)
    {
        await EnsureDeviceOwnedByUserAsync(userId, deviceId, cancellationToken);

        var config = GetTimeseriesHourlyTableConfig();
        var table = QualifyTable(config);
        var ownershipFilter = BuildOwnershipFilterSql(GetDevicesTableConfig(), "t.device_id");

        var query = $"""
                     SELECT *
                     FROM (
                       SELECT
                         t.hour_ts,
                         t.avg_temperature,
                         t.min_temperature,
                         t.max_temperature,
                         t.avg_moisture,
                         t.min_moisture,
                         t.max_moisture,
                         t.avg_conductivity,
                         t.min_conductivity,
                         t.max_conductivity,
                         t.avg_ph_value,
                         t.min_ph_value,
                         t.max_ph_value,
                         t.sample_count
                       FROM {table} t
                      WHERE t.device_id = @deviceId
                        AND {ownershipFilter}
                        AND (@fromTs IS NULL OR t.hour_ts >= @fromTs)
                        AND (@toTs IS NULL OR t.hour_ts <= @toTs)
                       ORDER BY t.hour_ts DESC
                       LIMIT @limit
                     )
                     ORDER BY hour_ts ASC
                     """;

        var parameters = BuildOwnershipParameters(userId, deviceId).ToList();
        parameters.Add(new BigQueryParameter("limit", BigQueryDbType.Int64, limit));
        parameters.Add(new BigQueryParameter("fromTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(from)));
        parameters.Add(new BigQueryParameter("toTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(to)));

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, parameters, cancellationToken: cancellationToken);
        var points = rows.Select(MapTimeseriesPoint).ToList();
        return new DeviceTimeseriesResponseDto(deviceId, "hour", points);
    }

    public async Task<DeviceTrendsResponseDto> GetDeviceTrendsDailyForUserAsync(
        string userId,
        string deviceId,
        int limit,
        DateTimeOffset? from,
        DateTimeOffset? to,
        CancellationToken cancellationToken)
    {
        await EnsureDeviceOwnedByUserAsync(userId, deviceId, cancellationToken);

        var config = GetTrendsDailyTableConfig();
        var table = QualifyTable(config);
        var ownershipFilter = BuildOwnershipFilterSql(GetDevicesTableConfig(), "t.device_id");

        var query = $"""
                     SELECT *
                     FROM (
                       SELECT
                         t.day,
                         t.slope_temperature,
                         t.slope_moisture,
                         t.slope_conductivity,
                         t.slope_ph_value,
                         t.temperature_direction,
                         t.moisture_direction,
                         t.conductivity_direction,
                         t.ph_direction,
                         t.computed_at
                       FROM {table} t
                      WHERE t.device_id = @deviceId
                        AND {ownershipFilter}
                        AND (@fromTs IS NULL OR t.day >= DATE(@fromTs))
                        AND (@toTs IS NULL OR t.day <= DATE(@toTs))
                       ORDER BY t.day DESC
                       LIMIT @limit
                     )
                     ORDER BY day ASC
                     """;

        var parameters = BuildOwnershipParameters(userId, deviceId).ToList();
        parameters.Add(new BigQueryParameter("limit", BigQueryDbType.Int64, limit));
        parameters.Add(new BigQueryParameter("fromTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(from)));
        parameters.Add(new BigQueryParameter("toTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(to)));

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, parameters, cancellationToken: cancellationToken);
        var points = rows.Select(MapTrendPoint).ToList();
        return new DeviceTrendsResponseDto(deviceId, "day", points);
    }

    public async Task<IReadOnlyList<OutOfRangeEventDto>> GetOutOfRangeEventsForUserAsync(
        string userId,
        string deviceId,
        int limit,
        DateTimeOffset? from,
        DateTimeOffset? to,
        CancellationToken cancellationToken)
    {
        await EnsureDeviceOwnedByUserAsync(userId, deviceId, cancellationToken);

        var config = GetOutOfRangeEventsTableConfig();
        var table = QualifyTable(config);
        var ownershipFilter = BuildOwnershipFilterSql(GetDevicesTableConfig(), "e.device_id");

        var query = $"""
                     SELECT
                       e.device_id,
                       e.metric,
                       e.start_time,
                       e.end_time,
                       e.min_allowed,
                       e.max_allowed,
                       e.observed_min,
                       e.observed_max,
                       e.severity,
                       e.status,
                       e.created_at,
                       e.updated_at
                     FROM {table} e
                     WHERE e.device_id = @deviceId
                       AND {ownershipFilter}
                       AND (@fromTs IS NULL OR e.start_time >= @fromTs)
                       AND (@toTs IS NULL OR e.start_time <= @toTs)
                     ORDER BY e.start_time DESC
                     LIMIT @limit
                     """;

        var parameters = BuildOwnershipParameters(userId, deviceId).ToList();
        parameters.Add(new BigQueryParameter("limit", BigQueryDbType.Int64, limit));
        parameters.Add(new BigQueryParameter("fromTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(from)));
        parameters.Add(new BigQueryParameter("toTs", BigQueryDbType.Timestamp, ToNullableUtcDateTime(to)));

        var client = BigQueryClient.Create(config.ProjectId);
        var rows = await client.ExecuteQueryAsync(query, parameters, cancellationToken: cancellationToken);
        return rows.Select(MapOutOfRangeEvent).ToList();
    }

    private static async Task EnsureDeviceOwnedByUserAsync(string userId, string deviceId, CancellationToken cancellationToken)
    {
        var config = GetDevicesTableConfig();
        var table = QualifyTable(config);
        var query = $"""
                     SELECT 1
                     FROM {table}
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

        if (!rows.Any())
        {
            throw new DeviceNotFoundForUserException(deviceId);
        }
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

    private static TableConfig GetStateLatestTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_STATE_LATEST_DATASET");
        var table = GetRequiredEnv("BQ_STATE_LATEST_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "state_latest");
    }

    private static TableConfig GetTimeseriesHourlyTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_TIMESERIES_HOURLY_DATASET");
        var table = GetRequiredEnv("BQ_TIMESERIES_HOURLY_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "timeseries_hourly");
    }

    private static TableConfig GetTrendsDailyTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_TRENDS_DAILY_DATASET");
        var table = GetRequiredEnv("BQ_TRENDS_DAILY_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "trends_daily");
    }

    private static TableConfig GetOutOfRangeEventsTableConfig()
    {
        var projectId = ProjectId;
        var dataset = GetRequiredEnv("BQ_OUT_OF_RANGE_EVENTS_DATASET");
        var table = GetRequiredEnv("BQ_OUT_OF_RANGE_EVENTS_TABLE");

        return ValidateAndBuild(projectId, dataset, table, "out_of_range_events");
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

    private static string BuildOwnershipFilterSql(TableConfig devicesTableConfig, string deviceIdColumnReference)
    {
        var devicesTable = QualifyTable(devicesTableConfig);
        return $"EXISTS (SELECT 1 FROM {devicesTable} d WHERE d.device_id = {deviceIdColumnReference} AND d.user_id = @userId)";
    }

    private static IReadOnlyList<BigQueryParameter> BuildOwnershipParameters(string userId, string deviceId)
    {
        return new[]
        {
            new BigQueryParameter("deviceId", BigQueryDbType.String, deviceId),
            new BigQueryParameter("userId", BigQueryDbType.String, userId)
        };
    }

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

    private static DeviceStateLatestDto MapStateLatest(BigQueryRow row)
    {
        return new DeviceStateLatestDto(
            DeviceId: ToNullableString(row["device_id"]) ?? string.Empty,
            ReadingTime: ToIso8601Nullable(row["reading_time"]) ?? string.Empty,
            Temperature: ToNullableDouble(row["temperature"]),
            Moisture: ToNullableDouble(row["moisture"]),
            Conductivity: ToNullableDouble(row["conductivity"]),
            PhValue: ToNullableDouble(row["ph_value"]),
            NpkContent: ToNullableString(row["npk_content"]),
            TemperatureLow: ToNullableBool(row["temperature_low"]),
            TemperatureHigh: ToNullableBool(row["temperature_high"]),
            MoistureLow: ToNullableBool(row["moisture_low"]),
            MoistureHigh: ToNullableBool(row["moisture_high"]),
            ConductivityLow: ToNullableBool(row["conductivity_low"]),
            ConductivityHigh: ToNullableBool(row["conductivity_high"]),
            PhLow: ToNullableBool(row["ph_low"]),
            PhHigh: ToNullableBool(row["ph_high"]),
            RiskScore: ToNullableInt(row["risk_score"]),
            AiStatus: ToNullableString(row["ai_status"]),
            TemperatureTrend: ToNullableString(row["temperature_trend"]),
            MoistureTrend: ToNullableString(row["moisture_trend"]),
            ConductivityTrend: ToNullableString(row["conductivity_trend"]),
            PhTrend: ToNullableString(row["ph_trend"]),
            LastSeenMinutes: ToNullableInt(row["last_seen_minutes"]),
            UpdatedAt: ToIso8601Nullable(row["updated_at"]) ?? string.Empty);
    }

    private static DeviceTimeseriesPointDto MapTimeseriesPoint(BigQueryRow row)
    {
        return new DeviceTimeseriesPointDto(
            HourTs: ToIso8601Nullable(row["hour_ts"]) ?? string.Empty,
            AvgTemperature: ToNullableDouble(row["avg_temperature"]),
            MinTemperature: ToNullableDouble(row["min_temperature"]),
            MaxTemperature: ToNullableDouble(row["max_temperature"]),
            AvgMoisture: ToNullableDouble(row["avg_moisture"]),
            MinMoisture: ToNullableDouble(row["min_moisture"]),
            MaxMoisture: ToNullableDouble(row["max_moisture"]),
            AvgConductivity: ToNullableDouble(row["avg_conductivity"]),
            MinConductivity: ToNullableDouble(row["min_conductivity"]),
            MaxConductivity: ToNullableDouble(row["max_conductivity"]),
            AvgPhValue: ToNullableDouble(row["avg_ph_value"]),
            MinPhValue: ToNullableDouble(row["min_ph_value"]),
            MaxPhValue: ToNullableDouble(row["max_ph_value"]),
            SampleCount: ToNullableInt(row["sample_count"]) ?? 0);
    }

    private static DeviceTrendPointDto MapTrendPoint(BigQueryRow row)
    {
        return new DeviceTrendPointDto(
            Day: ToDateOnlyString(row["day"]) ?? string.Empty,
            SlopeTemperature: ToNullableDouble(row["slope_temperature"]),
            SlopeMoisture: ToNullableDouble(row["slope_moisture"]),
            SlopeConductivity: ToNullableDouble(row["slope_conductivity"]),
            SlopePhValue: ToNullableDouble(row["slope_ph_value"]),
            TemperatureDirection: ToNullableString(row["temperature_direction"]),
            MoistureDirection: ToNullableString(row["moisture_direction"]),
            ConductivityDirection: ToNullableString(row["conductivity_direction"]),
            PhDirection: ToNullableString(row["ph_direction"]),
            ComputedAt: ToIso8601Nullable(row["computed_at"]) ?? string.Empty);
    }

    private static OutOfRangeEventDto MapOutOfRangeEvent(BigQueryRow row)
    {
        return new OutOfRangeEventDto(
            DeviceId: ToNullableString(row["device_id"]) ?? string.Empty,
            Metric: ToNullableString(row["metric"]) ?? string.Empty,
            StartTime: ToIso8601Nullable(row["start_time"]) ?? string.Empty,
            EndTime: ToIso8601Nullable(row["end_time"]),
            MinAllowed: ToNullableDouble(row["min_allowed"]),
            MaxAllowed: ToNullableDouble(row["max_allowed"]),
            ObservedMin: ToNullableDouble(row["observed_min"]),
            ObservedMax: ToNullableDouble(row["observed_max"]),
            Severity: ToNullableString(row["severity"]),
            Status: ToNullableString(row["status"]),
            CreatedAt: ToIso8601Nullable(row["created_at"]) ?? string.Empty,
            UpdatedAt: ToIso8601Nullable(row["updated_at"]) ?? string.Empty);
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

    private static bool? ToNullableBool(object? value)
    {
        return value switch
        {
            null => null,
            bool b => b,
            string s when bool.TryParse(s, out var parsed) => parsed,
            _ => null
        };
    }

    private static string? ToDateOnlyString(object? value)
    {
        return value switch
        {
            null => null,
            DateTime dt => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            string s => s,
            _ => value.ToString()
        };
    }

    private static DateTime? ToNullableUtcDateTime(DateTimeOffset? value) =>
        value?.UtcDateTime;

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

internal sealed class DeviceNotFoundForUserException(string deviceId)
    : Exception($"Device '{deviceId}' was not found for the authenticated user.");
