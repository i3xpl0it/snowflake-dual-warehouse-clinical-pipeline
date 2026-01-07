{\rtf1\ansi\ansicpg1252\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 """\
Real-time Snowflake Cost Monitoring\
Tracks warehouse usage and alerts on anomalies\
"""\
\
import snowflake.connector\
import logging\
from typing import Dict, List\
from datetime import datetime, timedelta\
from config import SnowflakeConfig\
\
logging.basicConfig(level=logging.INFO)\
logger = logging.getLogger(__name__)\
\
class CostMonitor:\
    """Monitor and analyze Snowflake costs"""\
    \
    def __init__(self, config: SnowflakeConfig):\
        self.config = config\
        self.conn = None\
        self.cursor = None\
        \
    def connect(self):\
        """Establish connection"""\
        try:\
            self.conn = snowflake.connector.connect(**self.config.to_dict())\
            self.cursor = self.conn.cursor()\
            logger.info("Connected for cost monitoring")\
            return True\
        except Exception as e:\
            logger.error(f"Connection failed: \{e\}")\
            return False\
    \
    def get_warehouse_costs(self, days: int = 7) -> List[Dict]:\
        """Get warehouse costs for last N days"""\
        query = f"""\
        SELECT \
            warehouse_name,\
            DATE(start_time) as date,\
            SUM(credits_used) as total_credits,\
            SUM(credits_used) * 3 as estimated_cost_usd,\
            COUNT(*) as query_count\
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY\
        WHERE start_time >= DATEADD(DAY, -\{days\}, CURRENT_TIMESTAMP())\
          AND warehouse_name IN (\
              'CLINICAL_INIT_WH', \
              'CLINICAL_CDC_WH', \
              'CLINICAL_INTERACTIVE_WH'\
          )\
        GROUP BY warehouse_name, DATE(start_time)\
        ORDER BY date DESC, warehouse_name;\
        """\
        \
        try:\
            self.cursor.execute(query)\
            costs = []\
            for row in self.cursor:\
                costs.append(\{\
                    'warehouse': row[0],\
                    'date': row[1],\
                    'credits': float(row[2]),\
                    'cost_usd': float(row[3]),\
                    'queries': int(row[4])\
                \})\
            logger.info(f"Retrieved cost data for \{len(costs)\} warehouse-days")\
            return costs\
        except Exception as e:\
            logger.error(f"Error getting costs: \{e\}")\
            return []\
    \
    def get_daily_summary(self) -> Dict:\
        """Get today's cost summary"""\
        query = """\
        SELECT \
            SUM(credits_used) as total_credits,\
            SUM(credits_used) * 3 as total_cost_usd,\
            COUNT(DISTINCT warehouse_name) as warehouses_used,\
            COUNT(*) as total_queries\
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY\
        WHERE DATE(start_time) = CURRENT_DATE()\
          AND warehouse_name LIKE 'CLINICAL%';\
        """\
        \
        try:\
            self.cursor.execute(query)\
            row = self.cursor.fetchone()\
            summary = \{\
                'total_credits': float(row[0] or 0),\
                'total_cost_usd': float(row[1] or 0),\
                'warehouses_used': int(row[2] or 0),\
                'total_queries': int(row[3] or 0),\
                'date': datetime.now().strftime('%Y-%m-%d')\
            \}\
            logger.info(f"Today's cost: $\{summary['total_cost_usd']:.2f\}")\
            return summary\
        except Exception as e:\
            logger.error(f"Error getting daily summary: \{e\}")\
            return \{\}\
    \
    def detect_anomalies(self, threshold_multiplier: float = 2.0) -> List[Dict]:\
        """Detect cost anomalies"""\
        query = f"""\
        WITH daily_costs AS (\
            SELECT \
                DATE(start_time) as date,\
                warehouse_name,\
                SUM(credits_used) as credits\
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY\
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())\
            GROUP BY DATE(start_time), warehouse_name\
        ),\
        averages AS (\
            SELECT \
                warehouse_name,\
                AVG(credits) as avg_credits,\
                STDDEV(credits) as stddev_credits\
            FROM daily_costs\
            GROUP BY warehouse_name\
        )\
        SELECT \
            dc.date,\
            dc.warehouse_name,\
            dc.credits,\
            a.avg_credits,\
            (dc.credits - a.avg_credits) / NULLIF(a.stddev_credits, 0) as z_score\
        FROM daily_costs dc\
        JOIN averages a ON dc.warehouse_name = a.warehouse_name\
        WHERE dc.credits > a.avg_credits * \{threshold_multiplier\}\
        ORDER BY dc.date DESC;\
        """\
        \
        try:\
            self.cursor.execute(query)\
            anomalies = []\
            for row in self.cursor:\
                anomalies.append(\{\
                    'date': row[0],\
                    'warehouse': row[1],\
                    'credits': float(row[2]),\
                    'avg_credits': float(row[3]),\
                    'z_score': float(row[4]) if row[4] else 0\
                \})\
            if anomalies:\
                logger.warning(f"Found \{len(anomalies)\} cost anomalies!")\
            return anomalies\
        except Exception as e:\
            logger.error(f"Error detecting anomalies: \{e\}")\
            return []\
    \
    def close(self):\
        """Close connections"""\
        if self.cursor:\
            self.cursor.close()\
        if self.conn:\
            self.conn.close()\
\
def main():\
    """Main execution"""\
    config = SnowflakeConfig.from_env()\
    monitor = CostMonitor(config)\
    \
    if monitor.connect():\
        # Daily summary\
        summary = monitor.get_daily_summary()\
        print(f"\\n\uc0\u55357 \u56496  Today's Cost Summary (\{summary.get('date', 'N/A')\}):")\
        print(f"  Credits: \{summary.get('total_credits', 0):.2f\}")\
        print(f"  Est. Cost: $\{summary.get('total_cost_usd', 0):.2f\}")\
        print(f"  Queries: \{summary.get('total_queries', 0)\}")\
        \
        # Week costs\
        costs = monitor.get_warehouse_costs(days=7)\
        if costs:\
            print(f"\\n\uc0\u55357 \u56522  Last 7 Days by Warehouse:")\
            for cost in costs[:5]:  # Show last 5 entries\
                print(f"  \{cost['warehouse']\} (\{cost['date']\}): $\{cost['cost_usd']:.2f\}")\
        \
        # Anomalies\
        anomalies = monitor.detect_anomalies()\
        if anomalies:\
            print(f"\\n\uc0\u9888 \u65039   Cost Anomalies Detected:")\
            for anomaly in anomalies:\
                print(f"  \{anomaly['date']\} - \{anomaly['warehouse']\}: \{anomaly['credits']:.2f\} credits (avg: \{anomaly['avg_credits']:.2f\})")\
        \
        monitor.close()\
\
if __name__ == '__main__':\
    main()\
}