package part2;

public class ShipStats
{
    public int count = 1;
    public double maxlon = 0;
    public double minlon = 0;
    public double maxlat = 0;
    public double minlat = 0;
    public long maxt = 0;
    public long mint = 0;

    public ShipStats(int count, double maxlon, double minlon, double maxlat, double minlat, long maxt, long mint) {
        this.count = count;
        this.maxlon = maxlon;
        this.minlon = minlon;
        this.maxlat = maxlat;
        this.minlat = minlat;
        this.maxt = maxt;
        this.mint = mint;
    }
}
