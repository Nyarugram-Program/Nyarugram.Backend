using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public sealed record WallpaperColors(uint Background, uint? Second, uint? Third, uint? Fourth,
    int? Rotation, int? Intensity, bool Blur, bool Motion, string? Emoticon);
public sealed record WallpaperDefinition(long Id, WallpaperColors Settings, bool Dark, bool IsDefault);
public sealed record WallpaperChanged(string UserId, long Revision, string? ConversationId) : IEventPayload;

public static class WallpaperService
{
    public static WallpaperDefinition Parse(P.Wallpaper? input)
    {
        if (input is null) throw new ApiException(400, "WALLPAPER_REQUIRED");
        if (input.KindCase != P.Wallpaper.KindOneofCase.NoFile)
            throw new ApiException(501, "BOTGRAM_WALLPAPER_KIND_UNSUPPORTED");
        var settings = input.NoFile.Settings;
        if (settings is null || !settings.HasBackgroundColor)
            throw new ApiException(400, "WALLPAPER_BACKGROUND_REQUIRED");
        if (settings.HasEmoticon && settings.Emoticon.Length != 0)
            throw new ApiException(501, "BOTGRAM_WALLPAPER_EMOTICON_UNSUPPORTED");
        if (settings.BackgroundColor > 0xffffff ||
            settings.HasSecondBackgroundColor && settings.SecondBackgroundColor > 0xffffff ||
            settings.HasThirdBackgroundColor && settings.ThirdBackgroundColor > 0xffffff ||
            settings.HasFourthBackgroundColor && settings.FourthBackgroundColor > 0xffffff ||
            settings.HasSecondBackgroundColor != settings.HasRotation ||
            settings.HasThirdBackgroundColor && !settings.HasSecondBackgroundColor ||
            settings.HasFourthBackgroundColor && !settings.HasThirdBackgroundColor ||
            settings.HasRotation && settings.Rotation is < 0 or > 359 ||
            settings.HasIntensity && settings.Intensity is < -100 or > 100)
            throw new ApiException(400, "WALLPAPER_SETTINGS_INVALID");
        return new(input.Id, new(settings.BackgroundColor,
            settings.HasSecondBackgroundColor ? settings.SecondBackgroundColor : null,
            settings.HasThirdBackgroundColor ? settings.ThirdBackgroundColor : null,
            settings.HasFourthBackgroundColor ? settings.FourthBackgroundColor : null,
            settings.HasRotation ? settings.Rotation : null, settings.HasIntensity ? settings.Intensity : null,
            settings.Blur, settings.Motion, settings.HasEmoticon ? settings.Emoticon : null),
            input.NoFile.Dark, input.NoFile.IsDefault);
    }

    public static P.Wallpaper Map(WallpaperDefinition wallpaper)
    {
        var value = wallpaper.Settings;
        var settings = new P.WallpaperSettings { BackgroundColor = value.Background, Blur = value.Blur, Motion = value.Motion };
        if (value.Second is { } second) settings.SecondBackgroundColor = second;
        if (value.Third is { } third) settings.ThirdBackgroundColor = third;
        if (value.Fourth is { } fourth) settings.FourthBackgroundColor = fourth;
        if (value.Rotation is { } rotation) settings.Rotation = rotation;
        if (value.Intensity is { } intensity) settings.Intensity = intensity;
        if (value.Emoticon is { } emoticon) settings.Emoticon = emoticon;
        return new() { Id = wallpaper.Id,
            NoFile = new() { Settings = settings, Dark = wallpaper.Dark, IsDefault = wallpaper.IsDefault } };
    }

    public static P.WallpaperState State(WallpaperDefinition? value, bool overridden = false) =>
        value is null ? new() { Cleared = new(), Overridden = overridden } :
            new() { Wallpaper = Map(value), Overridden = overridden };

    public static string Key(WallpaperDefinition value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value))));

    public static WallpaperDefinition Read(string json) =>
        JsonSerializer.Deserialize<WallpaperDefinition>(json) ?? throw new InvalidDataException("Missing stored wallpaper");

    public static void ValidateSet(P.SetChatWallpaperRequest input)
    {
        if (input.Peer is null) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        if (input.SelectionCase == P.SetChatWallpaperRequest.SelectionOneofCase.ServiceMessageId)
            throw new ApiException(501, "BOTGRAM_WALLPAPER_SERVICE_MESSAGE_UNSUPPORTED");
        if (input.Revert ? input.SelectionCase != P.SetChatWallpaperRequest.SelectionOneofCase.None || input.ForBoth :
            input.SelectionCase is not (P.SetChatWallpaperRequest.SelectionOneofCase.Wallpaper or
                P.SetChatWallpaperRequest.SelectionOneofCase.Clear))
            throw new ApiException(400, "WALLPAPER_SELECTION_INVALID");
        if (input.SelectionCase == P.SetChatWallpaperRequest.SelectionOneofCase.Wallpaper) Parse(input.Wallpaper);
    }
}
