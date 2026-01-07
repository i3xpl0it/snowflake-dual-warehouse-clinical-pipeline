{\rtf1\ansi\ansicpg1252\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww13800\viewh10100\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 """\
Configuration Management for Snowflake Clinical Pipeline\
Handles environment variables and connection parameters\
"""\
\
import os\
from typing import Dict, Optional\
from dataclasses import dataclass\
\
@dataclass\
class SnowflakeConfig:\
    """Snowflake connection configuration"""\
    user: str\
    password: str\
    account: str\
    warehouse_init: str\
    warehouse_cdc: str\
    warehouse_interactive: str\
    database: str\
    schema: str\
    role: str\
    \
    @classmethod\
    def from_env(cls):\
        """Load configuration from environment variables"""\
        return cls(\
            user=os.getenv('SNOWFLAKE_USER', ''),\
            password=os.getenv('SNOWFLAKE_PASSWORD', ''),\
            account=os.getenv('SNOWFLAKE_ACCOUNT', ''),\
            warehouse_init=os.getenv('WAREHOUSE_INIT', 'CLINICAL_INIT_WH'),\
            warehouse_cdc=os.getenv('WAREHOUSE_CDC', 'CLINICAL_CDC_WH'),\
            warehouse_interactive=os.getenv('WAREHOUSE_INTERACTIVE', 'CLINICAL_INTERACTIVE_WH'),\
            database=os.getenv('SNOWFLAKE_DATABASE', 'CLINICAL_DATA_PIPELINE'),\
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA'),\
            role=os.getenv('SNOWFLAKE_ROLE', 'CLINICAL_DATA_ENGINEER')\
        )\
    \
    def to_dict(self) -> Dict[str, str]:\
        """Convert to dictionary for snowflake.connector"""\
        return \{\
            'user': self.user,\
            'password': self.password,\
            'account': self.account,\
            'warehouse': self.warehouse_init,\
            'database': self.database,\
            'schema': self.schema,\
            'role': self.role\
        \}\
\
# Example usage\
if __name__ == '__main__':\
    config = SnowflakeConfig.from_env()\
    print(f"Configured for account: \{config.account\}")\
}