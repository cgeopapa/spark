package part2.model;

import com.vividsolutions.jts.geom.Coordinate;

import java.io.Serializable;

public class Ship2 implements Serializable
{
    public String id;
    public String course;
    public Coordinate coordinate;
    public long timestamp;
    public String heading;
    public double speed = 0;

    public Ship2(String id, String course, Coordinate coordinate, long timestamp, String heading)
    {
        this.id = id;
        this.course = course;
        this.coordinate = coordinate;
        this.timestamp = timestamp;
        this.heading = heading;
    }
}
