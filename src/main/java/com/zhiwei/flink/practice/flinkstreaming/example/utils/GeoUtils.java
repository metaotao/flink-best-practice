package com.zhiwei.flink.practice.flinkstreaming.example.utils;

public class GeoUtils {

    // geo boundaries of the area of NYC
    public static final double LON_EAST = -73.7;
    public static final double LON_WEST = -74.05;
    public static final double LAT_NORTH = 41.0;
    public static final double LAT_SOUTH = 40.5;

    // area width and height
    public static final double LON_WIDTH = 74.05 - 73.7;
    public static final double LAT_HEIGHT = 41.0 - 40.5;

    // delta step to create artificial grid overlay of NYC
    public static final double DELTA_LON = 0.0014;
    public static final double DELTA_LAT = 0.00125;

    // ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
    public static final int NUMBER_OF_GRID_X = 250;
    // ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON
    public static final int NUMBER_OF_GRID_Y = 400;

    public static final float DEG_LEN = 110.25f;

    /**
     * Checks if a location specified by longitude and latitude values is
     * within the geo boundaries of New York City.
     *
     * @param lon longitude of the location to check
     * @param lat latitude of the location to check
     *
     * @return true if the location is within NYC boundaries, otherwise false.
     */
    public static boolean isInNYC(float lon, float lat) {

        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }



}
