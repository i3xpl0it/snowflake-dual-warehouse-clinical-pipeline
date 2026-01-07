{\rtf1\ansi\ansicpg1252\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww13800\viewh10100\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 """\
PostgreSQL CDC Orchestrator for Snowflake Pipeline\
Manages CDC stream processing and Dynamic Table refreshes\
"""\
\
import snowflake.connector\
import logging\
from typing import List, Dict\
from datetime import datetime\
from config import SnowflakeConfig\
\
logging.basicConfig(level=logging.INFO)\
logger = logging.getLogger(__name__)\
\
class CDCOrchestrator:\
    """Manages PostgreSQL CDC to Snowflake streaming"""\
    \
    def __init__(self, config: SnowflakeConfig):\
        self.config = config\
        self.conn = None\
        self.cursor = None\
        \
    def connect(self):\
        """Establish Snowflake connection"""\
        try:\
            conn_params = self.config.to_dict()\
            conn_params['warehouse'] = self.config.warehouse_cdc  # Use CDC warehouse\
            self.conn = snowflake.connector.connect(**conn_params)\
            self.cursor = self.conn.cursor()\
            logger.info(f"Connected to Snowflake using \{self.config.warehouse_cdc\}")\
            return True\
        except Exception as e:\
            logger.error(f"Connection failed: \{e\}")\
            return False\
    \
    def check_stream_lag(self) -> List[Dict]:\
        """Check CDC stream processing lag"""\
        query = """\
        SELECT \
            stream_name,\
            table_name,\
            stale,\
            SYSTEM$STREAM_HAS_DATA(stream_name) as has_data\
        FROM INFORMATION_SCHEMA.STREAMS\
        WHERE schema_name = 'RAW_DATA'\
        ORDER BY stream_name;\
        """\
        \
        try:\
            self.cursor.execute(query)\
            streams = []\
            for row in self.cursor:\
                streams.append(\{\
                    'stream_name': row[0],\
                    'table_name': row[1],\
                    'is_stale': row[2],\
                    'has_data': row[3]\
                \})\
            logger.info(f"Checked \{len(streams)\} CDC streams")\
            return streams\
        except Exception as e:\
            logger.error(f"Error checking streams: \{e\}")\
            return []\
    \
    def get_stream_change_count(self, stream_name: str) -> int:\
        """Count pending changes in a stream"""\
        query = f"SELECT COUNT(*) FROM RAW_DATA.\{stream_name\};"\
        \
        try:\
            self.cursor.execute(query)\
            count = self.cursor.fetchone()[0]\
            logger.info(f"\{stream_name\}: \{count\} pending changes")\
            return count\
        except Exception as e:\
            logger.error(f"Error counting \{stream_name\}: \{e\}")\
            return 0\
    \
    def monitor_dynamic_tables(self) -> List[Dict]:\
        """Monitor Dynamic Table refresh status"""\
        query = """\
        SELECT \
            name,\
            target_lag,\
            refresh_mode,\
            last_refresh_time,\
            DATEDIFF(MINUTE, last_refresh_time, CURRENT_TIMESTAMP()) as minutes_since_refresh\
        FROM INFORMATION_SCHEMA.DYNAMIC_TABLES\
        WHERE schema_name = 'STAGING'\
        ORDER BY name;\
        """\
        \
        try:\
            self.cursor.execute(query)\
            tables = []\
            for row in self.cursor:\
                tables.append(\{\
                    'name': row[0],\
                    'target_lag': row[1],\
                    'refresh_mode': row[2],\
                    'last_refresh': row[3],\
                    'minutes_since_refresh': row[4]\
                \})\
            logger.info(f"Monitored \{len(tables)\} Dynamic Tables")\
            return tables\
        except Exception as e:\
            logger.error(f"Error monitoring Dynamic Tables: \{e\}")\
            return []\
    \
    def close(self):\
        """Close connections"""\
        if self.cursor:\
            self.cursor.close()\
        if self.conn:\
            self.conn.close()\
        logger.info("Connections closed")\
\
def main():\
    """Main execution"""\
    config = SnowflakeConfig.from_env()\
    orchestrator = CDCOrchestrator(config)\
    \
    if orchestrator.connect():\
        # Check stream status\
        streams = orchestrator.check_stream_lag()\
        print(f"\\n\uc0\u55357 \u56522  CDC Stream Status:")\
        for stream in streams:\
            print(f"  \{stream['stream_name']\}: \{'Has data' if stream['has_data'] else 'No data'\}")\
        \
        # Monitor Dynamic Tables\
        tables = orchestrator.monitor_dynamic_tables()\
        print(f"\\n\uc0\u9889  Dynamic Table Status:")\
        for table in tables:\
            print(f"  \{table['name']\}: Last refresh \{table['minutes_since_refresh']\} min ago")\
        \
        orchestrator.close()\
\
if __name__ == '__main__':\
    main()\
}