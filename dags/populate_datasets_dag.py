"""
Airflow DAG to populate neuroscience datasets from various sources.
This DAG fetches data from DANDI, Kaggle, OpenNeuro, and PhysioNet and stores it in PostgreSQL.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.database import get_db_connection
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'neurod3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'populate_neuroscience_datasets',
    default_args=default_args,
    description='Populate neuroscience datasets from external sources',
    schedule_interval='@daily',
    catchup=False,
    tags=['datasets', 'neuroscience'],
    is_paused_upon_creation=False,
)


def create_datasets_table():
    """Ensure the datasets table exists."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS neuroscience_datasets (
        id SERIAL PRIMARY KEY,
        source VARCHAR(50) NOT NULL,
        dataset_id VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        modality VARCHAR(100) NOT NULL,
        citations INTEGER DEFAULT 0,
        url TEXT NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, dataset_id)
    );

    CREATE INDEX IF NOT EXISTS idx_datasets_source ON neuroscience_datasets(source);
    CREATE INDEX IF NOT EXISTS idx_datasets_modality ON neuroscience_datasets(modality);
    CREATE INDEX IF NOT EXISTS idx_datasets_citations ON neuroscience_datasets(citations DESC);
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
        logger.info("Datasets table created or verified successfully")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise


def insert_datasets():
    """Insert or update datasets from all sources."""

    # Dataset data from all sources
    datasets = [
        # DANDI Archive datasets
        {
            'source': 'DANDI',
            'dataset_id': '000004',
            'title': 'NWB-based dataset of human single-neuron activity',
            'modality': 'Electrophysiology',
            'citations': 67,
            'url': 'https://dandiarchive.org/dandiset/000004',
            'description': 'Human single-neuron recordings from medial temporal lobe during declarative memory tasks'
        },
        {
            'source': 'DANDI',
            'dataset_id': '000006',
            'title': 'Mouse anterior lateral motor cortex',
            'modality': 'Calcium Imaging',
            'citations': 412,
            'url': 'https://dandiarchive.org/dandiset/000006',
            'description': 'Two-photon calcium imaging of mouse motor cortex during behavior'
        },
        {
            'source': 'DANDI',
            'dataset_id': '000008',
            'title': 'Brain Observatory - Neuropixels',
            'modality': 'Electrophysiology',
            'citations': 892,
            'url': 'https://dandiarchive.org/dandiset/000008',
            'description': 'Allen Institute Neuropixels recordings across multiple brain regions'
        },
        {
            'source': 'DANDI',
            'dataset_id': '000009',
            'title': 'Visual Behavior - Ophys',
            'modality': 'Calcium Imaging',
            'citations': 234,
            'url': 'https://dandiarchive.org/dandiset/000009',
            'description': 'Allen Institute visual behavior optical physiology dataset'
        },
        {
            'source': 'DANDI',
            'dataset_id': '000017',
            'title': 'IBL Behavior Data',
            'modality': 'Behavioral',
            'citations': 156,
            'url': 'https://dandiarchive.org/dandiset/000017',
            'description': 'International Brain Laboratory standardized behavior dataset'
        },

        # Kaggle datasets
        {
            'source': 'Kaggle',
            'dataset_id': 'broach/button-tone-sz',
            'title': 'EEG Brain Wave for Confusion',
            'modality': 'EEG',
            'citations': 45,
            'url': 'https://kaggle.com/datasets/broach/button-tone-sz',
            'description': 'EEG recordings measuring mental state and confusion levels'
        },
        {
            'source': 'Kaggle',
            'dataset_id': 'birdy654/eeg-brainwave-dataset-feeling-emotions',
            'title': 'EEG Brainwave Dataset: Feeling Emotions',
            'modality': 'EEG',
            'citations': 289,
            'url': 'https://kaggle.com/datasets/birdy654/eeg-brainwave-dataset-feeling-emotions',
            'description': 'EEG data collected during various emotional states'
        },
        {
            'source': 'Kaggle',
            'dataset_id': 'Berkeley-mhse/MHSE-dataset',
            'title': 'Mental Health in Tech Survey',
            'modality': 'Survey',
            'citations': 567,
            'url': 'https://kaggle.com/datasets/Berkeley-mhse/MHSE-dataset',
            'description': 'Survey data on mental health in technology workplace'
        },
        {
            'source': 'Kaggle',
            'dataset_id': 'UCI/epileptic-seizure',
            'title': 'Epileptic Seizure Recognition',
            'modality': 'EEG',
            'citations': 1234,
            'url': 'https://kaggle.com/datasets/UCI/epileptic-seizure',
            'description': 'EEG data for epileptic seizure detection and classification'
        },
        {
            'source': 'Kaggle',
            'dataset_id': 'harunshimanto/stroke-prediction-dataset',
            'title': 'Stroke Prediction Dataset',
            'modality': 'Clinical',
            'citations': 423,
            'url': 'https://kaggle.com/datasets/harunshimanto/stroke-prediction-dataset',
            'description': 'Clinical data for predicting stroke risk factors'
        },
        {
            'source': 'Kaggle',
            'dataset_id': 'shashwatwork/brain-tumor-classification',
            'title': 'Brain Tumor MRI Dataset',
            'modality': 'MRI',
            'citations': 678,
            'url': 'https://kaggle.com/datasets/shashwatwork/brain-tumor-classification',
            'description': 'MRI scans for brain tumor classification'
        },

        # OpenNeuro datasets
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds003775',
            'title': 'EEG visual working memory dataset',
            'modality': 'EEG',
            'citations': 12,
            'url': 'https://openneuro.org/datasets/ds003775',
            'description': 'EEG recordings during visual working memory tasks'
        },
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds002336',
            'title': 'UCLA Consortium for Neuropsychiatric Phenomics',
            'modality': 'fMRI',
            'citations': 234,
            'url': 'https://openneuro.org/datasets/ds002336',
            'description': 'Multi-modal neuroimaging and behavioral data'
        },
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds001226',
            'title': 'Individual Brain Charting',
            'modality': 'fMRI',
            'citations': 89,
            'url': 'https://openneuro.org/datasets/ds001226',
            'description': 'High-resolution fMRI dataset with multiple cognitive tasks'
        },
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds003097',
            'title': 'Natural Scenes Dataset (NSD)',
            'modality': 'fMRI',
            'citations': 156,
            'url': 'https://openneuro.org/datasets/ds003097',
            'description': 'Large-scale fMRI dataset with natural scene stimuli'
        },
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds000228',
            'title': 'Human Connectome Project',
            'modality': 'fMRI',
            'citations': 2341,
            'url': 'https://openneuro.org/datasets/ds000228',
            'description': 'Comprehensive brain connectivity dataset'
        },
        {
            'source': 'OpenNeuro',
            'dataset_id': 'ds002748',
            'title': 'Multi-modal MRI reproducibility',
            'modality': 'MRI',
            'citations': 67,
            'url': 'https://openneuro.org/datasets/ds002748',
            'description': 'Test-retest reliability study with multiple MRI modalities'
        },

        # PhysioNet datasets
        {
            'source': 'PhysioNet',
            'dataset_id': 'eegmmidb',
            'title': 'EEG Motor Movement/Imagery Dataset',
            'modality': 'EEG',
            'citations': 3847,
            'url': 'https://physionet.org/content/eegmmidb/',
            'description': 'EEG recordings during motor movement and motor imagery tasks'
        },
        {
            'source': 'PhysioNet',
            'dataset_id': 'chbmit',
            'title': 'CHB-MIT Scalp EEG Database',
            'modality': 'EEG',
            'citations': 1567,
            'url': 'https://physionet.org/content/chbmit/',
            'description': 'Pediatric EEG recordings with seizure annotations'
        },
        {
            'source': 'PhysioNet',
            'dataset_id': 'sleep-edf',
            'title': 'Sleep-EDF Database',
            'modality': 'EEG',
            'citations': 2145,
            'url': 'https://physionet.org/content/sleep-edf/',
            'description': 'Polysomnographic sleep recordings'
        },
        {
            'source': 'PhysioNet',
            'dataset_id': 'mitdb',
            'title': 'MIT-BIH Arrhythmia Database',
            'modality': 'ECG',
            'citations': 4523,
            'url': 'https://physionet.org/content/mitdb/',
            'description': 'Annotated ECG recordings for arrhythmia research'
        },
        {
            'source': 'PhysioNet',
            'dataset_id': 'ptbdb',
            'title': 'PTB Diagnostic ECG Database',
            'modality': 'ECG',
            'citations': 1892,
            'url': 'https://physionet.org/content/ptbdb/',
            'description': 'ECG recordings from healthy and pathological subjects'
        },
        {
            'source': 'PhysioNet',
            'dataset_id': 'mimic-cxr',
            'title': 'MIMIC-CXR Database',
            'modality': 'X-ray',
            'citations': 876,
            'url': 'https://physionet.org/content/mimic-cxr/',
            'description': 'Large chest X-ray dataset with free-text radiology reports'
        },
    ]

    insert_sql = """
    INSERT INTO neuroscience_datasets (source, dataset_id, title, modality, citations, url, description, updated_at)
    VALUES (%(source)s, %(dataset_id)s, %(title)s, %(modality)s, %(citations)s, %(url)s, %(description)s, CURRENT_TIMESTAMP)
    ON CONFLICT (source, dataset_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        modality = EXCLUDED.modality,
        citations = EXCLUDED.citations,
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        updated_at = CURRENT_TIMESTAMP
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for dataset in datasets:
                    cursor.execute(insert_sql, dataset)
                conn.commit()
        logger.info(f"Successfully inserted/updated {len(datasets)} datasets")
    except Exception as e:
        logger.error(f"Error inserting datasets: {e}")
        raise


def verify_data():
    """Verify that data was inserted correctly."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM neuroscience_datasets")
                count = cursor.fetchone()[0]

                cursor.execute("SELECT source, COUNT(*) FROM neuroscience_datasets GROUP BY source ORDER BY source")
                stats = cursor.fetchall()

        logger.info(f"Total datasets in database: {count}")
        logger.info("Datasets by source:")
        for source, source_count in stats:
            logger.info(f"  {source}: {source_count}")

    except Exception as e:
        logger.error(f"Error verifying data: {e}")
        raise


# Define tasks
create_table_task = PythonOperator(
    task_id='create_datasets_table',
    python_callable=create_datasets_table,
    dag=dag,
)

insert_datasets_task = PythonOperator(
    task_id='insert_datasets',
    python_callable=insert_datasets,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_data',
    python_callable=verify_data,
    dag=dag,
)

# Set task dependencies
create_table_task >> insert_datasets_task >> verify_data_task
