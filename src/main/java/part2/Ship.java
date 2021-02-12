package part2;

import java.io.Serializable;

public class Ship implements Serializable
{
    public String id;
    public String course;
    public double lon;
    public double lat;
    public long timestamp;
    public String heading;
    public String speed;
    public int count = 1;
    public double maxlon = 0;
    public double minlon = 0;
    public double maxlat = 0;
    public double minlat = 0;
    public long maxt = 0;
    public long mint = 0;

    public Ship(String id, String course, double lon, double lat, long timestamp, String heading, String speed)
    {
        this.id = id;
        this.course = course;
        this.lon = maxlon = minlon = lon;
        this.lat = maxlat = minlat = lat;
        this.timestamp = maxt = mint = timestamp;
        this.heading = heading;
        this.speed = speed;
    }

    public Ship(int count, double maxlon, double minlon, double maxlat, double minlat, long maxt, long mint) {
        this.count = count;
        this.maxlon = maxlon;
        this.minlon = minlon;
        this.maxlat = maxlat;
        this.minlat = minlat;
        this.maxt = maxt;
        this.mint = mint;
    }
}
