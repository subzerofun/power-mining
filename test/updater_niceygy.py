print("Loading...")
import zlib
import zmq
import simplejson
import sys
import time
from sqlalchemy import and_, create_engine, Column, Integer, String, Float, Boolean
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import math


"""
 "  Configuration
"""
__relayEDDN = "tcp://eddn.edcd.io:9500"
__timeoutEDDN = 600000
BUBBLE_LIMIT_LOW = -550
BUBBLE_LIMIT_HIGH = 550

DATABASE_URI = "mysql+pymysql://assistant:6548@10.0.0.52/elite"

Base = sqlalchemy.orm.declarative_base()

"""
TABLES:

star_systems:

    id: int pri key
    system_name text
    latitude float
    longitude float
    height float
    state text (powerplay state)
    shortcode text (power shortcode)
    is_anarchy bool
    has_res_sites bool

stations:

    id int pri key
    name text
    system text
    type text (Starport, Outpost, PlanetaryPort, Settlement, EngineerBase)

megaships: 
    name text pri key
    system1 text
    system2 text
    system3 text
    system4 text
    system5 text
    system6 text
"""


class StarSystem(Base):
    __tablename__ = "star_systems"
    system_name = Column(String(255), primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    height = Column(Float)
    state = Column(String(255))
    shortcode = Column(String(255))
    is_anarchy = Column(Boolean)
    has_res_sites = Column(Boolean)


class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_name = Column(String(255))
    star_system = Column(String(255))
    station_type = Column(String(255))


class Megaship(Base):
    __tablename__ = "megaships"
    name = Column(String(255), primary_key=True)
    SYSTEM1 = Column(String(255))
    SYSTEM2 = Column(String(255))
    SYSTEM3 = Column(String(255))
    SYSTEM4 = Column(String(255))
    SYSTEM5 = Column(String(255))
    SYSTEM6 = Column(String(255))


# Create an engine and a session
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()


def get_week_of_cycle():
    """
    Determines the current week of a cycle.
    Returns:
        int: The current week of the cycle (1-based).
    """
    date = datetime.now()
    days_since_start = (date - datetime(2025, 1, 9)).days
    weeks = math.trunc(days_since_start / 7)
    weeks = weeks + 1
    while weeks > 6:
        weeks = weeks - 6
    return  weeks


print(f"Today is week {get_week_of_cycle()} in a 6-week cycle.")


def create_tables():
    """
    Create the necessary tables in the database if they do not exist.
    """
    print("Creating tables")
    Base.metadata.create_all(engine)
    print("Tables created")


def add_system(
    session,
    system_name,
    latitude,
    longitude,
    height,
    state,
    shortcode,
    is_anarchy,
    # has_res_sites,
):
    # Is in bubble?
    if (
        latitude > BUBBLE_LIMIT_HIGH
        or latitude < BUBBLE_LIMIT_LOW
        or longitude > BUBBLE_LIMIT_HIGH
        or longitude < BUBBLE_LIMIT_LOW
        or height > BUBBLE_LIMIT_HIGH
        or height < BUBBLE_LIMIT_LOW
    ):
        # ignore
        return
    else:
        system_name = str(system_name).replace("'", ".")
        # is already in database?
        system = session.query(StarSystem).filter_by(system_name=system_name).first()
        if system is None:
            # print(f"Adding {system_name}")
            # not already in db, add it
            new_system = StarSystem(
                system_name=system_name,
                latitude=latitude,
                longitude=longitude,
                height=height,
                state=state,
                shortcode=shortcode,
                is_anarchy=is_anarchy,
            )
            session.add(new_system)
        else:
            if system.height is None:
                system.height = height
                system.latitude = latitude,
                system.longitude = longitude
                system.shortcode = shortcode
                system.state = state
                system.is_anarchy = is_anarchy
            else:
                # already in db, update
                system.shortcode = shortcode
                system.state = state
                system.is_anarchy = is_anarchy
                # print(f"Updating system {system_name}")
                # system.has_res_sites = has_res_sites


def add_station(session, station_name, station_type, system_name):
    system_name = str(system_name).replace("'", ".")
    # is already in database?
    station = session.query(Station).filter(
        and_(Station.star_system == system_name, Station.station_name == station_name)
    ).first()

    # station type
    match station_type:
        case "Coriolis":
            station_type = "Starport"
        case "Orbis":
            station_type = "Starport"
        case "Ocellus":
            station_type = "Starport"

    if station is None:
        # not already in db, add it
        # print(f"adding {station_name}")
        new_station = Station(
            star_system=system_name,
            station_name=station_name,
            station_type=station_type,
        )
        session.add(new_station)
    # else:
    #     print(f"skipping {station_name}")


def alter_system_data(session, system_name, has_res_sites=None, is_anarchy=None):
    system_name = str(system_name).replace("'", ".")
    system = session.query(StarSystem).filter_by(system_name=system_name).first()
    if system is None:
        new_system = StarSystem(
            system_name=system_name, has_res_sites=has_res_sites, is_anarchy=is_anarchy
        )
        print(f"Added new system: {system_name}")
        session.add(new_system)
    else:
        if has_res_sites is not None:
            system.has_res_sites = has_res_sites
        if is_anarchy is not None:
            system.is_anarchy = is_anarchy


def add_megaship(megaship_name, system, session):
    # print(f"Adding megaship {megaship_name} in {system} for week {get_week_of_cycle()}")
    # what week is it?

    week = get_week_of_cycle()
    system_mapping = {
        1: "SYSTEM1",
        2: "SYSTEM2",
        3: "SYSTEM3",
        4: "SYSTEM4",
        5: "SYSTEM5",
        6: "SYSTEM6",
    }

    megaship = session.query(Megaship).filter_by(name=megaship_name).first()
    # print("megaship")
    if megaship is not None:
        # entry exists
        system_attribute = system_mapping.get(week)
        if (
            system_attribute is not None
            and getattr(megaship, system_attribute) is None
        ):
            # entry for this week does not exist, update it
            match week:
                case 1:
                    megaship.SYSTEM1 = system
                case 2:
                    megaship.SYSTEM2 = system
                case 3:
                    megaship.SYSTEM3 = system
                case 4:
                    megaship.SYSTEM4 = system
                case 5:
                    megaship.SYSTEM5 = system
                case 6:
                    megaship.SYSTEM6 = system
            session.add(megaship)
        else:
            message = "already gotdata for this week, ignoring"
    else:
        new_megaship = Megaship()

        if week in system_mapping:
            new_megaship = Megaship(
                name=megaship_name, **{system_mapping[week]: system}
            )
            session.add(new_megaship)
            return
        else:
            raise ValueError("Invalid week number")


def main():
    time.sleep(5)
    print("Starting...")
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    print("Using database URI: " + DATABASE_URI)
    create_tables()

    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)

    try:
        subscriber.connect(__relayEDDN)

        while True:
            try:
                __message = subscriber.recv()

                if __message == False:
                    subscriber.disconnect(__relayEDDN)
                    break

                __message = zlib.decompress(__message)
                __json = simplejson.loads(__message)

                if "event" in __json["message"]:
                    match __json["message"]["event"]:
                        case "CarrierJumpRequest":
                            print(__json["message"])

                        # case "Docked":
                        #     systemName = str(__json["message"]["StarSystem"])
                        #     stationName = str(__json["message"]["StationName"])
                        #     stationType = str(__json["message"]["StationType"])
                        #     factionName = str(
                        #         __json["message"]["StationFaction"]["Name"]
                        #     )
                        #     isAnarchy = False
                        #     if (
                        #         str(__json["message"]["StationGovernment"])
                        #         == "$government_Anarchy;"
                        #     ):
                        #         isAnarchy = True

                        #     if (
                        #         stationType != "FleetCarrier"
                        #         and stationName != "OnFootSettlement"
                        #     ):
                        #         add_station(
                        #             session,
                        #             stationName,
                        #             stationType,
                        #             systemName,
                        #         )
                        #         alter_system_data(session, systemName, None, isAnarchy)

                        case "FSSSignalDiscovered":
                            for signal in __json["message"]["signals"]:
                                if "SignalType" in signal:
                                    if signal["SignalType"] == "ResourceExtraction":
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        # alter_system_data(
                                        #     session, systemName, True, None
                                        # )
                                    elif signal["SignalType"] == "Megaship":
                                        megaship_name = str(signal["SignalName"])
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        add_megaship(megaship_name, systemName, session)
                                    elif signal["SignalType"] == "StationCoriolis":
                                        station_name = str(signal["SignalName"])
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        add_station(
                                            session,
                                            station_name,
                                            "Coriolis",
                                            systemName,
                                        )
                                    elif signal["SignalType"] == "Outpost":
                                        station_name = str(signal["SignalName"])
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        add_station(
                                            session,
                                            station_name,
                                            "Outpost",
                                            systemName,
                                        )
                                    elif signal["SignalType"] == "StationONeilOrbis":
                                        station_name = str(signal["SignalName"])
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        add_station(
                                            session, station_name, "Orbis", systemName
                                        )
                                    elif signal["SignalType"] == "Ocellus":
                                        station_name = str(signal["SignalName"])
                                        systemName = str(
                                            __json["message"]["StarSystem"]
                                        )
                                        add_station(
                                            session,
                                            station_name,
                                            "Ocellus",
                                            systemName,
                                        )

                        case "FSDJump":
                            starPos = __json["message"]["StarPos"]
                            if "ControllingPower" in __json["message"]:
                                power = __json["message"]["ControllingPower"]
                                latitude = starPos[1]
                                longitude = starPos[0]
                                height = starPos[2]
                                system_name = __json["message"]["StarSystem"]
                                state = __json["message"]["PowerplayState"]
                                security = __json["message"]["SystemSecurity"]
                                isAnarchy = False
                                if security == "$GAlAXY_MAP_INFO_state_anarchy;":
                                    isAnarchy = True
                                else:
                                    isAnarchy = False
                                shortcode = ""
                                match power:
                                    case "Edmund Mahon":
                                        shortcode = "EMH"
                                    case "A. Lavigny-Duval":
                                        shortcode = "ALD"
                                    case "Aisling Duval":
                                        shortcode = "ASD"
                                    case "Yuri Grom":
                                        shortcode = "YRG"
                                    case "Pranav Antal":
                                        shortcode = "PRA"
                                    case "Denton Patreus":
                                        shortcode = "DPT"
                                    case "Jerome Archer":
                                        shortcode = "JRA"
                                    case "Nakato Kaine":
                                        shortcode = "NAK"
                                    case "Archon Delane":
                                        shortcode = "ARD"
                                    case "Li Yong-Rui":
                                        shortcode = "LYR"
                                    case "Felicia Winters":
                                        shortcode = "FLW"
                                    case "Zemina Torval":
                                        shortcode = "ZMT"
                                    case _:
                                        shortcode = ""

                                add_system(
                                    session,
                                    system_name,
                                    latitude,
                                    longitude,
                                    height,
                                    state,
                                    shortcode,
                                    isAnarchy,
                                )

                session.commit()
                # commit once per cycle, not once per function

            except zmq.ZMQError as e:
                print("ZMQSocketException: " + str(e))
                sys.stdout.flush()
                subscriber.disconnect(__relayEDDN)
                time.sleep(5)
    except Exception as e:
        print("Error: " + str(e))
        sys.stdout.flush()
        time.sleep(5)


main()
