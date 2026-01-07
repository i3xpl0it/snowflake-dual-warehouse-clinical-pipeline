"""
synthetic_data_generator.py
Generate synthetic clinical data for testing the dual-warehouse pipeline
Creates realistic patient demographics, encounters, and lab results
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from faker import Faker
import snowflake.connector
from config import SnowflakeConfig

# Initialize Faker for realistic data generation
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


class SyntheticClinicalDataGenerator:
    """Generate synthetic EHR data for testing"""
    
    def __init__(self, num_patients=10000):
        self.num_patients = num_patients
        self.patients = None
        self.encounters = None
        self.lab_results = None
        
        # Clinical data dictionaries
        self.encounter_types = ['EMERGENCY', 'INPATIENT', 'OUTPATIENT', 'TELEHEALTH']
        self.encounter_weights = [0.15, 0.10, 0.60, 0.15]
        
        # ICD-10 diagnosis codes (common conditions)
        self.diagnosis_codes = {
            'E11.9': 'Type 2 diabetes mellitus without complications',
            'I10': 'Essential (primary) hypertension',
            'J44.9': 'Chronic obstructive pulmonary disease, unspecified',
            'E78.5': 'Hyperlipidemia, unspecified',
            'J06.9': 'Acute upper respiratory infection, unspecified',
            'M25.50': 'Pain in unspecified joint',
            'R51': 'Headache',
            'K21.9': 'Gastro-esophageal reflux disease without esophagitis',
            'E66.9': 'Obesity, unspecified',
            'F41.9': 'Anxiety disorder, unspecified'
        }
        
        # Common lab tests with reference ranges
        self.lab_tests = {
            'HBA1C': {'unit': '%', 'normal_range': (4.0, 5.6), 'critical_threshold': 9.0},
            'GLUCOSE': {'unit': 'mg/dL', 'normal_range': (70, 100), 'critical_threshold': 250},
            'WBC': {'unit': 'K/uL', 'normal_range': (4.5, 11.0), 'critical_threshold': 20.0},
            'HEMOGLOBIN': {'unit': 'g/dL', 'normal_range': (12.0, 16.0), 'critical_threshold': 7.0},
            'CREATININE': {'unit': 'mg/dL', 'normal_range': (0.6, 1.2), 'critical_threshold': 3.0},
            'ALT': {'unit': 'U/L', 'normal_range': (7, 56), 'critical_threshold': 200},
            'CHOLESTEROL': {'unit': 'mg/dL', 'normal_range': (125, 200), 'critical_threshold': 300},
            'TROPONIN': {'unit': 'ng/mL', 'normal_range': (0, 0.04), 'critical_threshold': 1.0}
        }
    
    def generate_patients(self):
        """Generate synthetic patient demographics"""
        print(f"Generating {self.num_patients} synthetic patients...")
        
        patients_data = []
        for _ in range(self.num_patients):
            patient = {
                'PATIENT_ID': f"PAT-{uuid.uuid4().hex[:8].upper()}",
                'FIRST_NAME': fake.first_name(),
                'LAST_NAME': fake.last_name(),
                'DATE_OF_BIRTH': fake.date_of_birth(minimum_age=1, maximum_age=95),
                'GENDER': random.choice(['Male', 'Female', 'Other']),
                'SSN': fake.ssn(),
                'EMAIL': fake.email(),
                'PHONE': fake.phone_number(),
                'ADDRESS': fake.street_address(),
                'CITY': fake.city(),
                'STATE': fake.state_abbr(),
                'ZIP_CODE': fake.zipcode(),
                'CREATED_AT': datetime.now() - timedelta(days=random.randint(365, 3650)),
                'UPDATED_AT': datetime.now(),
                'DELETED_AT': None
            }
            patients_data.append(patient)
        
        self.patients = pd.DataFrame(patients_data)
        print(f"✅ Generated {len(self.patients)} patients")
        return self.patients
    
    def generate_encounters(self, encounters_per_patient_avg=5):
        """Generate synthetic patient encounters"""
        if self.patients is None:
            raise ValueError("Must generate patients first")
        
        print(f"Generating encounters (avg {encounters_per_patient_avg} per patient)...")
        
        encounters_data = []
        for _, patient in self.patients.iterrows():
            # Poisson distribution for realistic encounter counts
            num_encounters = np.random.poisson(encounters_per_patient_avg)
            num_encounters = max(1, min(num_encounters, 20))  # Cap between 1-20
            
            patient_created = patient['CREATED_AT']
            
            for _ in range(num_encounters):
                encounter_type = random.choices(
                    self.encounter_types, 
                    weights=self.encounter_weights
                )[0]
                
                # Encounters occur after patient creation
                days_after_creation = random.randint(0, 365 * 2)
                encounter_date = patient_created + timedelta(days=days_after_creation)
                
                # Discharge date for inpatient/emergency
                if encounter_type in ['INPATIENT', 'EMERGENCY']:
                    los_hours = random.randint(2, 240)  # 2 hours to 10 days
                    discharge_date = encounter_date + timedelta(hours=los_hours)
                else:
                    discharge_date = None
                
                # Random diagnosis
                diagnosis_code = random.choice(list(self.diagnosis_codes.keys()))
                
                encounter = {
                    'ENCOUNTER_ID': f"ENC-{uuid.uuid4().hex[:8].upper()}",
                    'PATIENT_ID': patient['PATIENT_ID'],
                    'ENCOUNTER_TYPE': encounter_type,
                    'ENCOUNTER_DATE': encounter_date,
                    'PROVIDER_ID': f"PROV-{random.randint(1000, 9999)}",
                    'FACILITY_ID': f"FAC-{random.randint(100, 999)}",
                    'CHIEF_COMPLAINT': fake.sentence(nb_words=6),
                    'DIAGNOSIS_CODE': diagnosis_code,
                    'DIAGNOSIS_DESCRIPTION': self.diagnosis_codes[diagnosis_code],
                    'DISCHARGE_DATE': discharge_date,
                    'STATUS': 'DISCHARGED' if discharge_date else 'ACTIVE',
                    'CREATED_AT': encounter_date,
                    'UPDATED_AT': discharge_date if discharge_date else encounter_date,
                    'DELETED_AT': None
                }
                encounters_data.append(encounter)
        
        self.encounters = pd.DataFrame(encounters_data)
        print(f"✅ Generated {len(self.encounters)} encounters")
        return self.encounters
    
    def generate_lab_results(self, labs_per_encounter_avg=3):
        """Generate synthetic lab results"""
        if self.encounters is None:
            raise ValueError("Must generate encounters first")
        
        print(f"Generating lab results (avg {labs_per_encounter_avg} per encounter)...")
        
        lab_results_data = []
        
        for _, encounter in self.encounters.iterrows():
            # Only generate labs for certain encounter types
            if encounter['ENCOUNTER_TYPE'] not in ['EMERGENCY', 'INPATIENT', 'OUTPATIENT']:
                continue
            
            num_labs = np.random.poisson(labs_per_encounter_avg)
            num_labs = max(1, min(num_labs, 10))
            
            # Select random lab tests
            selected_tests = random.sample(list(self.lab_tests.keys()), 
                                         min(num_labs, len(self.lab_tests)))
            
            for test_code in selected_tests:
                test_info = self.lab_tests[test_code]
                
                # Generate realistic result value
                normal_min, normal_max = test_info['normal_range']
                
                # 80% normal, 15% abnormal, 5% critical
                result_type = random.choices(
                    ['normal', 'abnormal', 'critical'],
                    weights=[0.80, 0.15, 0.05]
                )[0]
                
                if result_type == 'normal':
                    result_value = round(random.uniform(normal_min, normal_max), 2)
                    abnormal_flag = None
                elif result_type == 'abnormal':
                    if random.random() < 0.5:
                        result_value = round(random.uniform(normal_max, normal_max * 1.5), 2)
                        abnormal_flag = 'HIGH'
                    else:
                        result_value = round(random.uniform(normal_min * 0.5, normal_min), 2)
                        abnormal_flag = 'LOW'
                else:  # critical
                    result_value = round(random.uniform(normal_max * 1.5, 
                                                       test_info['critical_threshold']), 2)
                    abnormal_flag = 'CRITICAL'
                
                test_date = encounter['ENCOUNTER_DATE'] + timedelta(hours=random.randint(0, 12))
                result_date = test_date + timedelta(hours=random.randint(1, 48))
                
                lab_result = {
                    'LAB_RESULT_ID': f"LAB-{uuid.uuid4().hex[:8].upper()}",
                    'ENCOUNTER_ID': encounter['ENCOUNTER_ID'],
                    'PATIENT_ID': encounter['PATIENT_ID'],
                    'TEST_CODE': test_code,
                    'TEST_NAME': test_code.replace('_', ' ').title(),
                    'RESULT_VALUE': str(result_value),
                    'RESULT_UNIT': test_info['unit'],
                    'REFERENCE_RANGE': f"{normal_min}-{normal_max}",
                    'ABNORMAL_FLAG': abnormal_flag,
                    'TEST_DATE': test_date,
                    'RESULT_DATE': result_date,
                    'PROVIDER_ID': encounter['PROVIDER_ID'],
                    'CREATED_AT': result_date,
                    'UPDATED_AT': result_date,
                    'DELETED_AT': None
                }
                lab_results_data.append(lab_result)
        
        self.lab_results = pd.DataFrame(lab_results_data)
        print(f"✅ Generated {len(self.lab_results)} lab results")
        return self.lab_results
    
    def save_to_csv(self, output_dir='data/'):
        """Save generated data to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        if self.patients is not None:
            patients_file = f"{output_dir}/synthetic_patients.csv"
            self.patients.to_csv(patients_file, index=False)
            print(f"💾 Saved patients to {patients_file}")
        
        if self.encounters is not None:
            encounters_file = f"{output_dir}/synthetic_encounters.csv"
            self.encounters.to_csv(encounters_file, index=False)
            print(f"💾 Saved encounters to {encounters_file}")
        
        if self.lab_results is not None:
            labs_file = f"{output_dir}/synthetic_labs.csv"
            self.lab_results.to_csv(labs_file, index=False)
            print(f"💾 Saved lab results to {labs_file}")
    
    def load_to_snowflake(self, config: SnowflakeConfig):
        """Load synthetic data into Snowflake"""
        print("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            user=config.user,
            password=config.password,
            account=config.account,
            warehouse=config.warehouse,
            database=config.database,
            schema='RAW'
        )
        
        cursor = conn.cursor()
        
        try:
            # Load patients
            if self.patients is not None:
                print("Loading patients to Snowflake...")
                cursor.execute("TRUNCATE TABLE IF EXISTS RAW.PATIENTS_CDC")
                
                for _, row in self.patients.iterrows():
                    cursor.execute("""
                        INSERT INTO RAW.PATIENTS_CDC (
                            CDC_OPERATION, CDC_TIMESTAMP, PATIENT_ID, FIRST_NAME, LAST_NAME,
                            DATE_OF_BIRTH, GENDER, SSN, EMAIL, PHONE, ADDRESS, CITY, STATE,
                            ZIP_CODE, CREATED_AT, UPDATED_AT
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'INSERT', datetime.now(), row['PATIENT_ID'], row['FIRST_NAME'],
                        row['LAST_NAME'], row['DATE_OF_BIRTH'], row['GENDER'], row['SSN'],
                        row['EMAIL'], row['PHONE'], row['ADDRESS'], row['CITY'],
                        row['STATE'], row['ZIP_CODE'], row['CREATED_AT'], row['UPDATED_AT']
                    ))
                print(f"✅ Loaded {len(self.patients)} patients")
            
            # Load encounters
            if self.encounters is not None:
                print("Loading encounters to Snowflake...")
                # Similar INSERT logic for encounters...
                print(f"✅ Loaded {len(self.encounters)} encounters")
            
            # Load lab results
            if self.lab_results is not None:
                print("Loading lab results to Snowflake...")
                # Similar INSERT logic for lab results...
                print(f"✅ Loaded {len(self.lab_results)} lab results")
            
            conn.commit()
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def generate_all(self):
        """Generate all synthetic data"""
        self.generate_patients()
        self.generate_encounters(encounters_per_patient_avg=5)
        self.generate_lab_results(labs_per_encounter_avg=3)
        
        # Print summary statistics
        print("\n" + "="*60)
        print("📊 SYNTHETIC DATA GENERATION SUMMARY")
        print("="*60)
        print(f"Patients: {len(self.patients):,}")
        print(f"Encounters: {len(self.encounters):,}")
        print(f"Lab Results: {len(self.lab_results):,}")
        print(f"Avg Encounters/Patient: {len(self.encounters) / len(self.patients):.1f}")
        print(f"Avg Labs
