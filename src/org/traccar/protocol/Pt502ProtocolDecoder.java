/*
 * Copyright 2012 - 2017 Anton Tananaev (anton@traccar.org)
 * Copyright 2012 Luis Parada (luis.parada@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.protocol;

import org.jboss.netty.channel.Channel;
import org.traccar.BaseProtocolDecoder;
import org.traccar.DeviceSession;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.model.Position;
import org.traccar.Context;

import java.net.SocketAddress;
import java.util.regex.Pattern;
import java.util.Date;
import java.nio.ByteBuffer;

public class Pt502ProtocolDecoder extends BaseProtocolDecoder {

    private static final int MAX_CHUNK_SIZE = 960;

    private byte[] photo;

    private static final byte HEADER_CMD         =  0;
    private static final byte HEADER_GID         =  1;
    private static final byte HEADER_TIME        =  2;
    private static final byte HEADER_FIX_FLAG    =  3;
    private static final byte HEADER_LATITUDE    =  4;
    private static final byte HEADER_NORTH_SOUTH =  5;
    private static final byte HEADER_LONGITUDE   =  6;
    private static final byte HEADER_EAST_WEST   =  7;
    private static final byte HEADER_SPEED       =  8;
    private static final byte HEADER_HEADING     =  9;
    private static final byte HEADER_DATE        = 10;
    private static final byte HEADER_END         = 13;

    public Pt502ProtocolDecoder(Pt502Protocol protocol) {
        super(protocol);
    }

    private static final Pattern PATTERN = new PatternBuilder()
            .any().text("$")
            .expression("([^,]+),")              // type
            .number("(d+),")                     // id
            .number("(dd)(dd)(dd).(ddd),")       // time (hhmmss.sss)
            .expression("([AV]),")               // validity
            .number("(d+)(dd.dddd),")            // latitude
            .expression("([NS]),")
            .number("(d+)(dd.dddd),")            // longitude
            .expression("([EW]),")
            .number("(d+.d+)?,")                 // speed
            .number("(d+.d+)?,")                 // course
            .number("(dd)(dd)(dd),,,")           // date (ddmmyy)
            .expression("./")
            .expression("([01])+,")              // input
            .expression("([01])+/")              // output
            .expression("([^/]+)?/")             // adc
            .number("(d+)")                      // odometer
            .expression("/([^/]+)?/")            // rfid
            .number("(xxx)").optional(2)         // state
            .any()
            .compile();

    private String decodeAlarm(String value) {
        switch (value) {
            case "IN1":
                return Position.ALARM_SOS;
            case "GOF":
                return Position.ALARM_GEOFENCE;
            case "TOW":
                return Position.ALARM_TOW;
            case "HDA":
                return Position.ALARM_ACCELERATION;
            case "HDB":
                return Position.ALARM_BRAKING;
            case "FDA":
                return Position.ALARM_FATIGUE_DRIVING;
            case "SKA":
                return Position.ALARM_VIBRATION;
            case "PMA":
                return Position.ALARM_MOVEMENT;
            case "CPA":
                return Position.ALARM_POWER_CUT;
            default:
                return null;
        }
    }

    public String getCoordinateString(double coordinate) {
        String sign = coordinate > 0 ? "N" : "S";
        int deg = (int) Math.floor(Math.abs(coordinate));
        double min = 60 * Math.abs(coordinate % 1);

        return String.format("%d%07.4f%s", deg, min, sign);
    }

    public double overwriteCoordinate(double original, int index, String data) {
        String prevStr = getCoordinateString(original);
        String newLatStr = prevStr.substring(0, index) + data + prevStr.charAt(prevStr.length() - 1);

        Pattern p = new PatternBuilder()
            .number("(d+)(dd.dddd)")
            .expression("([NS])")
            .compile();
        Parser parser = new Parser(p, newLatStr);

        if (!parser.matches()) {
            return 0;
        }

        return parser.nextCoordinate();
    }

    private Position decodeDelta(
            Channel channel, SocketAddress remoteAddress, ByteBuffer msg, Position position) throws Exception {
        if (msg.get() != '@') {
            return null;
        }

        // try to set this position's initial device id
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);

        if (deviceSession == null) {
            return null;
        }
        if (deviceSession.getDeviceId() == 0) {
            return null;
        }

        position.setDeviceId(deviceSession.getDeviceId());

        // Skip second byte whose purpose is still unknown
        msg.get();

        boolean gotNewInfo = false;

        // need last position because we are processing deltas
        Position lastPosition = Context.getIdentityManager().getLastPosition(position.getDeviceId());

        while (msg.hasRemaining()) {
            byte infoType = msg.get();
            int overwriteIndex = msg.get();
            int infoLength = msg.get();

            byte[] rawInfo = new byte[infoLength];
            msg.get(rawInfo);
            String strInfo = new String(rawInfo);

            switch (infoType) {
                case HEADER_CMD:
                    String alarm = decodeAlarm(strInfo);

                    if (alarm != null) {
                        position.set(Position.KEY_ALARM, decodeAlarm(strInfo));
                        gotNewInfo = true;
                    }
                    break;

                case HEADER_GID:
                    // get previous device unique id
                    String currentUniqueID = Context.getIdentityManager().getById(position.getDeviceId()).getUniqueId();

                    // try to compute new unique id
                    if (overwriteIndex > currentUniqueID.length()) {
                        break;
                    }

                    String newUniqueID = currentUniqueID.substring(0, overwriteIndex) + strInfo;

                    // try to get new device session given new unique id
                    DeviceSession newDeviceSession = getDeviceSession(channel, remoteAddress, newUniqueID);

                    if (newDeviceSession == null) {
                        break;
                    }

                    long newDeviceID = newDeviceSession.getDeviceId();

                    if (newDeviceID != 0) {
                        position.setDeviceId(newDeviceID);
                        gotNewInfo = true;
                    }
                    break;

                case HEADER_LATITUDE:
                    if (lastPosition == null) {
                        break;
                    }

                    double newLatitude = overwriteCoordinate(lastPosition.getLatitude(), overwriteIndex, strInfo);
                    if (newLatitude != 0) {
                        position.setLatitude(newLatitude);

                        gotNewInfo = true;
                    }
                    break;

                case HEADER_LONGITUDE:
                    if (lastPosition == null) {
                        break;
                    }

                    double newLongitude = overwriteCoordinate(lastPosition.getLongitude(), overwriteIndex, strInfo);
                    if (newLongitude != 0) {
                        position.setLongitude(newLongitude);

                        gotNewInfo = true;
                    }
                    break;

                default: break;
            }
        }

        if (!gotNewInfo) {
            return null;
        }

        fillMissing(position, lastPosition);

        if (position.getDeviceTime() == null) {
            position.setDeviceTime(new Date());
        }
        if (position.getFixTime() == null) {
            position.setFixTime(new Date(0));
        }

        return position;
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        Position position = new Position();
        position.setProtocol(getProtocolName());

        String strdata = (String) msg;
        Parser parser = new Parser(PATTERN, strdata);

        if (!parser.matches()) {
            return decodeDelta(channel, remoteAddress, ByteBuffer.wrap(strdata.getBytes()), position);
        }

        String type = parser.next();

        if (type.startsWith("PHO") && channel != null) {
            photo = new byte[Integer.parseInt(type.substring(3))];
            channel.write("#PHD0," + Math.min(photo.length, MAX_CHUNK_SIZE) + "\r\n");
        }

        position.set(Position.KEY_ALARM, decodeAlarm(type));

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, parser.next());
        if (deviceSession == null) {
            return null;
        }
        position.setDeviceId(deviceSession.getDeviceId());

        DateBuilder dateBuilder = new DateBuilder()
                .setTime(parser.nextInt(0), parser.nextInt(0), parser.nextInt(0), parser.nextInt(0));

        position.setValid(parser.next().equals("A"));
        position.setLatitude(parser.nextCoordinate());
        position.setLongitude(parser.nextCoordinate());
        position.setSpeed(parser.nextDouble(0));
        position.setCourse(parser.nextDouble(0));

        dateBuilder.setDateReverse(parser.nextInt(0), parser.nextInt(0), parser.nextInt(0));
        position.setTime(dateBuilder.getDate());

        position.set(Position.KEY_INPUT, parser.next());
        position.set(Position.KEY_OUTPUT, parser.next());

        if (parser.hasNext()) {
            String[] values = parser.next().split(",");
            for (int i = 0; i < values.length; i++) {
                position.set(Position.PREFIX_ADC + (i + 1), Integer.parseInt(values[i], 16));
            }
        }

        position.set(Position.KEY_ODOMETER, parser.nextInt(0));
        position.set(Position.KEY_DRIVER_UNIQUE_ID, parser.next());

        if (parser.hasNext()) {
            int value = parser.nextHexInt(0);
            position.set(Position.KEY_BATTERY, value >> 8);
            position.set(Position.KEY_RSSI, (value >> 4) & 0xf);
            position.set(Position.KEY_SATELLITES, value & 0xf);
        }

        return position;
    }

}
