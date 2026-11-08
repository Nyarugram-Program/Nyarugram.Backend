namespace Botgram.Backend.Storage;

/// <summary>
/// Reads pixel dimensions from the first part of an uploaded image. Only headers are
/// inspected (bounded by the part already in memory); unknown or invalid input returns 0×0.
/// </summary>
public static class MediaImage
{
    public static (int Width, int Height) Dimensions(byte[] head, string mime)
    {
        if (head is null || head.Length < 16) return (0, 0);
        try
        {
            if (IsJpeg(head, mime)) return Jpeg(head);
            if (IsPng(head)) return Png(head);
            if (IsGif(head)) return Gif(head);
            if (IsWebp(head)) return Webp(head);
        }
        catch (IndexOutOfRangeException) { }
        return (0, 0);
    }

    private static bool IsJpeg(byte[] data, string mime) =>
        (data[0] == 0xFF && data[1] == 0xD8) || mime is "image/jpeg" or "image/jpg";

    private static bool IsPng(byte[] data) =>
        data.Length >= 24 && data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47;

    private static bool IsGif(byte[] data) =>
        data.Length >= 10 && data[0] == 'G' && data[1] == 'I' && data[2] == 'F' && data[3] == '8';

    private static bool IsWebp(byte[] data) =>
        data.Length >= 30 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F'
        && data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P';

    private static (int, int) Jpeg(byte[] data)
    {
        var index = 2; // after SOI
        while (index + 9 < data.Length)
        {
            if (data[index] != 0xFF) { index++; continue; }
            var marker = data[index + 1];
            if (marker == 0xD8 || marker == 0x01 || marker is >= 0xD0 and <= 0xD7) { index += 2; continue; }
            if (marker == 0xD9 || marker == 0xDA) break;
            var length = (data[index + 2] << 8) | data[index + 3];
            if (length < 2) break;
            // SOF0..SOF15 except DHT (C4), JPG (C8) and DAC (CC) carry the frame size.
            if (marker is >= 0xC0 and <= 0xCF && marker is not (0xC4 or 0xC8 or 0xCC))
                return (((data[index + 7] << 8) | data[index + 8]), ((data[index + 5] << 8) | data[index + 6]));
            index += 2 + length;
        }
        return (0, 0);
    }

    private static (int, int) Png(byte[] data) =>
        (((data[16] << 24) | (data[17] << 16) | (data[18] << 8) | data[19]),
         ((data[20] << 24) | (data[21] << 16) | (data[22] << 8) | data[23]));

    private static (int, int) Gif(byte[] data) =>
        ((data[6] | (data[7] << 8)), (data[8] | (data[9] << 8)));

    private static (int, int) Webp(byte[] data)
    {
        var chunk = System.Text.Encoding.ASCII.GetString(data, 12, 4);
        if (chunk == "VP8X")
            return (1 + (data[24] | (data[25] << 8) | (data[26] << 16)),
                    1 + (data[27] | (data[28] << 8) | (data[29] << 16)));
        if (chunk == "VP8L")
        {
            var width = 1 + (data[21] | ((data[22] & 0x3F) << 8));
            var height = 1 + (((data[22] & 0xC0) >> 6) | (data[23] << 2) | ((data[24] & 0x0F) << 10));
            return (width, height);
        }
        // Lossy VP8: the key frame header starts at 20 with a 3-byte tag and the 9D 01 2A sync.
        if (data.Length >= 30 && data[23] == 0x9D && data[24] == 0x01 && data[25] == 0x2A)
            return ((data[26] | (data[27] << 8)) & 0x3FFF, (data[28] | (data[29] << 8)) & 0x3FFF);
        return (0, 0);
    }
}
