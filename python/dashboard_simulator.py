"""
dashboard_simulator.py
Simulate clinical dashboard query workload to test dual-warehouse performance
Measures query latency and validates <100ms target with Interactive Tables
"""

import snowflake.connector
from snowflake.connector import DictCursor
import time
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from config import SnowflakeConfig
import random


class DashboardSimulator:
    """Simulate clinical dashboard queries and measure performance"""
    
    def __init__(self, config: SnowflakeConfig, warehouse='CLINICAL_INTERACTIVE_WH'):
        self.config = config
        self.warehouse = warehouse
        self.connection = None
        self.query_results = []
        
        # Dashboard queries to simulate
        self.dashboard_queries = {
            'patient_summary': """
                SELECT
                    p.PATIENT_ID,
                    p.FIRST_NAME || ' ' || p.LAST_NAME AS PATIENT_NAME,
                    DATEDIFF('YEAR', p.DATE_OF_BIRTH, CURRENT_DATE()) AS AGE,
                    p.GENDER,
                    COUNT(DISTINCT e.ENCOUNTER_ID) AS TOTAL_ENCOUNTERS,
                    MAX(e.ENCOUNTER_DATE) AS LAST_VISIT_DATE
                FROM CLINICAL_DB.CURATED.PATIENTS p
                LEFT JOIN CLINICAL_DB.CURATED.ENCOUNTERS e ON p.PATIENT_ID = e.PATIENT_ID
                WHERE p.PATIENT_ID = %(patient_id)s
                GROUP BY 1, 2, 3, 4
            """,
            
            'recent_encounters': """
                SELECT
                    e.ENCOUNTER_ID,
                    e.ENCOUNTER_TYPE,
                    e.ENCOUNTER_DATE,
                    e.DIAGNOSIS_DESCRIPTION,
                    e.STATUS
                FROM CLINICAL_DB.CURATED.ENCOUNTERS e
                WHERE e.PATIENT_ID = %(patient_id)s
                ORDER BY e.ENCOUNTER_DATE DESC
                LIMIT 10
            """,
            
            'recent_labs': """
                SELECT
                    l.TEST_DATE,
                    l.TEST_NAME,
                    l.RESULT_VALUE,
                    l.RESULT_UNIT,
                    l.REFERENCE_RANGE,
                    l.ABNORMAL_FLAG
                FROM CLINICAL_DB.CURATED.LAB_RESULTS l
                WHERE l.PATIENT_ID = %(patient_id)s
                ORDER BY l.TEST_DATE DESC
                LIMIT 20
            """,
            
            'ed_census': """
                SELECT
                    DATE_TRUNC('HOUR', e.ENCOUNTER_DATE) AS HOUR,
                    COUNT(*) AS ED_VISITS,
                    COUNT(DISTINCT e.PATIENT_ID) AS UNIQUE_PATIENTS
                FROM CLINICAL_DB.CURATED.ENCOUNTERS e
                WHERE e.ENCOUNTER_TYPE = 'EMERGENCY'
                    AND e.ENCOUNTER_DATE >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
                GROUP BY 1
                ORDER BY 1 DESC
            """,
            
            'high_risk_alerts': """
                SELECT
                    p.PATIENT_ID,
                    p.FIRST_NAME || ' ' || p.LAST_NAME AS PATIENT_NAME,
                    l.TEST_NAME,
                    l.RESULT_VALUE,
                    l.ABNORMAL_FLAG,
                    DATEDIFF('HOUR', l.TEST_DATE, CURRENT_TIMESTAMP()) AS HOURS_AGO
                FROM CLINICAL_DB.CURATED.LAB_RESULTS l
                INNER JOIN CLINICAL_DB.CURATED.PATIENTS p ON l.PATIENT_ID = p.PATIENT_ID
                WHERE l.ABNORMAL_FLAG = 'CRITICAL'
                    AND l.TEST_DATE >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
                ORDER BY l.TEST_DATE DESC
                LIMIT 50
            """
        }
    
    def connect(self):
        """Establish Snowflake connection"""
        print(f"Connecting to Snowflake warehouse: {self.warehouse}...")
        self.connection = snowflake.connector.connect(
            user=self.config.user,
            password=self.config.password,
            account=self.config.account,
            warehouse=self.warehouse,
            database=self.config.database,
            schema='CURATED'
        )
        print("✅ Connected")
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            print("Disconnected from Snowflake")
    
    def execute_query(self, query_name, query_sql, params=None):
        """Execute a single query and measure performance"""
        cursor = self.connection.cursor(DictCursor)
        
        start_time = time.time()
        
        try:
            if params:
                cursor.execute(query_sql, params)
            else:
                cursor.execute(query_sql)
            
            results = cursor.fetchall()
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Get query ID for detailed analysis
            query_id = cursor.sfqid
            
            result = {
                'query_name': query_name,
                'query_id': query_id,
                'execution_time_ms': round(execution_time_ms, 2),
                'row_count': len(results),
                'timestamp': datetime.now(),
                'success': True,
                'error': None
            }
            
            return result
            
        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            result = {
                'query_name': query_name,
                'query_id': None,
                'execution_time_ms': round(execution_time_ms, 2),
                'row_count': 0,
                'timestamp': datetime.now(),
                'success': False,
                'error': str(e)
            }
            return result
        
        finally:
            cursor.close()
    
    def get_sample_patient_ids(self, count=100):
        """Get sample patient IDs for testing"""
        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT PATIENT_ID 
            FROM CLINICAL_DB.CURATED.PATIENTS 
            LIMIT {count}
        """)
        patient_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return patient_ids
    
    def simulate_single_dashboard_load(self, patient_id):
        """Simulate loading a complete patient dashboard (3 queries)"""
        dashboard_results = []
        
        # Execute patient-specific queries
        for query_name in ['patient_summary', 'recent_encounters', 'recent_labs']:
            query_sql = self.dashboard_queries[query_name]
            result = self.execute_query(query_name, query_sql, {'patient_id': patient_id})
            dashboard_results.append(result)
        
        # Calculate total dashboard load time
        total_time = sum(r['execution_time_ms'] for r in dashboard_results)
        
        return {
            'patient_id': patient_id,
            'total_load_time_ms': round(total_time, 2),
            'query_results': dashboard_results
        }
    
    def simulate_concurrent_dashboards(self, num_concurrent=10, num_iterations=5):
        """Simulate multiple concurrent dashboard loads"""
        print(f"\n{'='*70}")
        print(f"🎯 SIMULATING {num_concurrent} CONCURRENT DASHBOARD LOADS")
        print(f"{'='*70}\n")
        
        if not self.connection:
            self.connect()
        
        # Get sample patient IDs
        patient_ids = self.get_sample_patient_ids(num_concurrent * num_iterations)
        
        all_results = []
        
        for iteration in range(num_iterations):
            print(f"Iteration {iteration + 1}/{num_iterations}...")
            
            # Select patients for this iteration
            start_idx = iteration * num_concurrent
            end_idx = start_idx + num_concurrent
            iteration_patients = patient_ids[start_idx:end_idx]
            
            # Execute concurrent dashboard loads
            with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
                futures = [
                    executor.submit(self.simulate_single_dashboard_load, patient_id)
                    for patient_id in iteration_patients
                ]
                
                for future in as_completed(futures):
                    result = future.result()
                    all_results.append(result)
                    
                    # Store individual query results
                    for query_result in result['query_results']:
                        self.query_results.append(query_result)
        
        return all_results
    
    def simulate_operational_queries(self, num_iterations=10):
        """Simulate operational dashboard queries (ED census, alerts)"""
        print(f"\n{'='*70}")
        print(f"📊 SIMULATING OPERATIONAL DASHBOARD QUERIES")
        print(f"{'='*70}\n")
        
        if not self.connection:
            self.connect()
        
        operational_results = []
        
        for iteration in range(num_iterations):
            print(f"Iteration {iteration + 1}/{num_iterations}...")
            
            # Execute ED census query
            ed_result = self.execute_query('ed_census', self.dashboard_queries['ed_census'])
            operational_results.append(ed_result)
            self.query_results.append(ed_result)
            
            # Execute high-risk alerts query
            alert_result = self.execute_query('high_risk_alerts', 
                                             self.dashboard_queries['high_risk_alerts'])
            operational_results.append(alert_result)
            self.query_results.append(alert_result)
            
            # Small delay between iterations
            time.sleep(0.5)
        
        return operational_results
    
    def generate_performance_report(self):
        """Generate comprehensive performance report"""
        if not self.query_results:
            print("No query results to analyze")
            return
        
        df = pd.DataFrame(self.query_results)
        
        print(f"\n{'='*70}")
        print(f"📈 DASHBOARD PERFORMANCE REPORT")
        print(f"{'='*70}\n")
        
        # Overall statistics
        successful_queries = df[df['success'] == True]
        
        print("OVERALL STATISTICS")
        print("-" * 70)
        print(f"Total Queries Executed: {len(df)}")
        print(f"Successful Queries: {len(successful_queries)} ({len(successful_queries)/len(df)*100:.1f}%)")
        print(f"Failed Queries: {len(df) - len(successful_queries)}")
        print()
        
        # Performance metrics
        if len(successful_queries) > 0:
            print("QUERY PERFORMANCE METRICS")
            print("-" * 70)
            
            for query_name in successful_queries['query_name'].unique():
                query_data = successful_queries[successful_queries['query_name'] == query_name]
                times = query_data['execution_time_ms'].tolist()
                
                print(f"\n{query_name}:")
                print(f"  Count: {len(times)}")
                print(f"  Mean: {statistics.mean(times):.2f}ms")
                print(f"  Median: {statistics.median(times):.2f}ms")
                print(f"  Min: {min(times):.2f}ms")
                print(f"  Max: {max(times):.2f}ms")
                print(f"  P95: {self.percentile(times, 95):.2f}ms")
                print(f"  P99: {self.percentile(times, 99):.2f}ms")
                
                # Check against <100ms target
                under_100ms = sum(1 for t in times if t < 100)
                pct_under_100ms = (under_100ms / len(times)) * 100
                
                if pct_under_100ms >= 95:
                    print(f"  ✅ {pct_under_100ms:.1f}% of queries < 100ms (TARGET MET)")
                else:
                    print(f"  ⚠️  {pct_under_100ms:.1f}% of queries < 100ms (TARGET: 95%)")
            
            # Overall latency distribution
            all_times = successful_queries['execution_time_ms'].tolist()
            print(f"\nOVERALL LATENCY DISTRIBUTION")
            print("-" * 70)
            print(f"Mean: {statistics.mean(all_times):.2f}ms")
            print(f"Median: {statistics.median(all_times):.2f}ms")
            print(f"P95: {self.percentile(all_times, 95):.2f}ms")
            print(f"P99: {self.percentile(all_times, 99):.2f}ms")
            
            under_100ms_pct = (sum(1 for t in all_times if t < 100) / len(all_times)) * 100
            print(f"\n{'='*70}")
            if under_100ms_pct >= 95:
                print(f"🎉 SUCCESS: {under_100ms_pct:.1f}% of queries under 100ms!")
                print(f"✅ Interactive Tables + Dual-Warehouse architecture meets SLA")
            else:
                print(f"📊 {under_100ms_pct:.1f}% of queries under 100ms")
                print(f"⚠️  Consider enabling caching or query optimization")
            print(f"{'='*70}\n")
        
        # Save detailed results to CSV
        results_file = f"dashboard_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(results_file, index=False)
        print(f"💾 Detailed results saved to: {results_file}")
    
    @staticmethod
    def percentile(data, percentile):
        """Calculate percentile"""
        size = len(data)
        return sorted(data)[int(size * percentile / 100)]
    
    def run_full_simulation(self, concurrent_dashboards=10, dashboard_iterations=5, 
                           operational_iterations=10):
        """Run complete dashboard simulation"""
        print("\n" + "="*70)
        print("🚀 STARTING DUAL-WAREHOUSE DASHBOARD SIMULATION")
        print("="*70)
        print(f"Warehouse: {self.warehouse}")
        print(f"Concurrent Users: {concurrent_dashboards}")
        print(f"Test Duration: ~{dashboard_iterations + operational_iterations} iterations")
        print("="*70 + "\n")
        
        try:
            self.connect()
            
            # Phase 1: Concurrent patient dashboards
            self.simulate_concurrent_dashboards(
                num_concurrent=concurrent_dashboards,
                num_iterations=dashboard_iterations
            )
            
            # Phase 2: Operational dashboards
            self.simulate_operational_queries(num_iterations=operational_iterations)
            
            # Generate performance report
            self.generate_performance_report()
            
        finally:
            self.disconnect()


def main():
    """Main execution"""
    #
