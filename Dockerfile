FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Botgram.Backend.csproj global.json packages.lock.json ./
RUN dotnet restore --locked-mode
COPY *.cs appsettings.json ./
COPY Protocol/BotgramRpc.cs ./Protocol/
COPY Resources/M49/botgram-m49.json Resources/M49/botgram-m49.sha256 ./Resources/M49/
RUN dotnet publish -c Release -o /out --no-restore
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /out .
ENV ASPNETCORE_URLS=http://+:8521
ENV DataPath=/data/botgram.json
EXPOSE 8521
ENTRYPOINT ["dotnet", "Botgram.Backend.dll", "--urls", "http://+:8521"]
