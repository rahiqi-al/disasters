import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from minio import Minio
import io
import os
import pickle
import logging
from batch.batchConfig.config import config


logger=logging.getLogger(__name__)

def train_and_track_model():


    minio_client = Minio("minio:9000", access_key=config.access_key, secret_key=config.secret_key, secure=False)


    # read csv from minio 
    try:
        response = minio_client.get_object(config.bucket_name_regression, config.csv_path_regression)
        df = pd.read_csv(io.BytesIO(response.read()))
        logger.info("Data read from MinIO")
    except Exception as e:
        logger.info(f"Error reading from MinIO: {e}")
        raise
    finally:
        response.close()
        response.release_conn()

    
    df['Classes'] = df['Classes'].str.strip().str.lower()
    df['Classes'] = df['Classes'].apply(lambda x: 1 if x == 'fire' else 0)
    x = df.drop(['FWI', 'day', 'month', 'year', 'Region', 'Classes'], axis=1)
    y = df['FWI']

    
    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.25, random_state=42)

    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    
    linreg = LinearRegression()
    linreg.fit(X_train_scaled, y_train)

    
    y_pred = linreg.predict(X_test_scaled)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    score = r2_score(y_test, y_pred)
    logger.info("Model trained")

    
    tracking_dir = "/mlflow/mlruns"
    os.makedirs(tracking_dir, exist_ok=True)
    mlflow.set_tracking_uri(f"file:{tracking_dir}")
    try:
        mlflow.set_experiment("fire_weather_index_prediction")
    except Exception as e:
        logger.exception(f"Error setting experiment: {e}")
        raise

    
    with mlflow.start_run() as run:
        mlflow.log_param("test_size", 0.25)
        mlflow.log_param("random_state", 42)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2_score", score)
        try:
            X_train_df = pd.DataFrame(X_train_scaled, columns=x.columns)
            signature = infer_signature(X_train_df, linreg.predict(X_test_scaled))
            mlflow.sklearn.log_model(linreg,"model",registered_model_name="LinearRegressionModel",signature=signature)
            scaler_path = "/tmp/scaler.pkl"
            with open(scaler_path, 'wb') as f:
                pickle.dump(scaler, f)
            mlflow.log_artifact(scaler_path, "scaler")
            logger.info(f"Model logged with run_id: {run.info.run_id}")

            # save to minio
            try:
                if not minio_client.bucket_exists(config.artifact_bucket_regression):
                    minio_client.make_bucket(config.artifact_bucket_regression)
                    logger.info(f"Created MinIO bucket: {config.artifact_bucket_regression}")
                model_path = f"/mlflow/mlruns/{run.info.experiment_id}/{run.info.run_id}/artifacts/model/model.pkl"
                minio_client.fput_object(config.artifact_bucket_regression, f"{run.info.run_id}/model.pkl", model_path)
                minio_client.fput_object(config.artifact_bucket_regression, f"{run.info.run_id}/scaler.pkl", scaler_path)
                logger.info(f"Saved to MinIO: {config.artifact_bucket_regression}/{run.info.run_id}")
            except Exception as e:
                logger.exception(f"MinIO save error: {e}")
                raise
        except Exception as e:
            logger.exception(f"Error logging to MLflow: {e}")
            raise
        finally:
            if os.path.exists(scaler_path):
                os.remove(scaler_path)
                logger.info("Cleaned up scaler file")