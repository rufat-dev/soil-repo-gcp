FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["src/SoilReportFn/SoilReportFn.csproj", "src/SoilReportFn/"]
RUN dotnet restore "src/SoilReportFn/SoilReportFn.csproj"

COPY . .
RUN dotnet publish "src/SoilReportFn/SoilReportFn.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://0.0.0.0:8080
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 8080

ENTRYPOINT ["dotnet", "SoilReportFn.dll"]
