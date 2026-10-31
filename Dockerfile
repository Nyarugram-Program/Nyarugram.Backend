FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Botgram.Backend.csproj global.json ./
RUN dotnet restore
COPY *.cs appsettings.json ./
COPY Resources/M49/botgram-m49.json Resources/M49/botgram-m49.sha256 ./Resources/M49/
RUN dotnet publish -c Release -o /out --no-restore
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://+:8080
ENV DataPath=/data/botgram.json
EXPOSE 8080
ENTRYPOINT ["dotnet", "Botgram.Backend.dll", "--urls", "http://+:8080"]
