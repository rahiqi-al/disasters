import tensorflow as tf
import numpy as np
from tensorflow.keras.applications import MobileNetV2
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D, Dropout
from tensorflow.keras.callbacks import EarlyStopping, TensorBoard
import mlflow
import mlflow.keras
from mlflow.models.signature import infer_signature
from minio import Minio
import os
import tempfile
import shutil
import logging
from batch.batchConfig.config import config


logger=logging.getLogger(__name__)

def cnn_model():
    gpus = tf.config.experimental.list_physical_devices('GPU')
    for gpu in gpus:
        tf.config.experimental.set_memory_growth(gpu, True)

    minio_client = Minio("minio:9000", access_key=config.access_key, secret_key=config.secret_key, secure=False)



    with tempfile.TemporaryDirectory(prefix="wildfire_dataset", dir="/tmp") as temp_dir:
        train_dir = os.path.join(temp_dir, "train")
        test_dir = os.path.join(temp_dir, "test")
        os.makedirs(train_dir, exist_ok=True)
        os.makedirs(test_dir, exist_ok=True)

        for folder in ["fire", "nofire"]:
            prefix = f"{config.base_path_cnn}/Training and Validation/{folder}/"
            local_folder = os.path.join(train_dir, folder)
            os.makedirs(local_folder, exist_ok=True)
            objects = minio_client.list_objects(config.bucket_name_cnn, prefix, recursive=True)
            for obj in objects:
                file_name = os.path.basename(obj.object_name)
                local_path = os.path.join(local_folder, file_name)
                minio_client.fget_object(config.bucket_name_cnn, obj.object_name, local_path)
                if "fire_0080.jpg" in file_name:
                    logger.info(f"Confirmed: Downloaded {local_path}")

        for folder in ["fire", "nofire"]:
            prefix = f"{config.base_path_cnn}/Testing/{folder}/"
            local_folder = os.path.join(test_dir, folder)
            os.makedirs(local_folder, exist_ok=True)
            objects = minio_client.list_objects(config.bucket_name_cnn, prefix, recursive=True)
            for obj in objects:
                file_name = os.path.basename(obj.object_name)
                local_path = os.path.join(local_folder, file_name)
                minio_client.fget_object(config.bucket_name_cnn, obj.object_name, local_path)

        logger.info(f"Data downloaded to {temp_dir}")
        logger.info(f"Training fire: {len(os.listdir(os.path.join(train_dir, 'fire')))} files")
        logger.info(f"Training nofire: {len(os.listdir(os.path.join(train_dir, 'nofire')))} files")
        logger.info(f"Testing fire: {len(os.listdir(os.path.join(test_dir, 'fire')))} files")
        logger.info(f"Testing nofire: {len(os.listdir(os.path.join(test_dir, 'nofire')))} files")

        train_size = 40
        val_size = 18
        train = tf.keras.utils.image_dataset_from_directory(train_dir, image_size=(256, 256), batch_size=32).map(lambda x, y: (x / 255, y)).take(train_size)
        val = tf.keras.utils.image_dataset_from_directory(train_dir, image_size=(256, 256), batch_size=32).map(lambda x, y: (x / 255, y)).skip(train_size).take(val_size)
        test = tf.keras.utils.image_dataset_from_directory(test_dir, image_size=(256, 256), batch_size=32).map(lambda x, y: (x / 255, y))

        base_model = MobileNetV2(input_shape=(256, 256, 3), include_top=False, weights='imagenet')
        base_model.trainable = False
        x = base_model.output
        x = GlobalAveragePooling2D()(x)
        x = Dropout(0.3)(x)
        x = Dense(128, activation='relu')(x)
        output = Dense(1, activation='sigmoid')(x)

        model = Model(inputs=base_model.input, outputs=output)

        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

        logger.info("Model compiled")

        tracking_dir = "/mlflow/mlruns"
        os.makedirs(tracking_dir, exist_ok=True)
        mlflow.set_tracking_uri(f"file:{tracking_dir}")
        mlflow.set_experiment("cnn_fire_prediction")

        with mlflow.start_run() as run:
            mlflow.log_param("train_size", train_size)
            mlflow.log_param("val_size", val_size)
            mlflow.log_param("epochs", 10)

            logdir = "/opt/airflow/logs"
            tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=logdir)
            early_stop = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)
            history = model.fit(train, epochs=10, validation_data=val, callbacks=[tensorboard_callback, early_stop])
           
            logger.info("Model trained")

            precision = tf.keras.metrics.Precision()
            recall = tf.keras.metrics.Recall()
            accuracy = tf.keras.metrics.BinaryAccuracy()
            for batch in test.as_numpy_iterator():
                X, y = batch
                yhat = model.predict(X)
                precision.update_state(y, yhat)
                recall.update_state(y, yhat)
                accuracy.update_state(y, yhat)
            metrics = {"precision": float(precision.result()),"recall": float(recall.result()),"accuracy": float(accuracy.result())}
            mlflow.log_metrics(metrics)

            logger.info(f"Metrics: {metrics}")

            for batch in test.take(1):
                X, _ = batch
                signature = infer_signature(X.numpy(), model.predict(X))
                break

            mlflow.keras.log_model(model, "model", registered_model_name="CNNModel", signature=signature)

            artifact_bucket = "mlflow"
            if not minio_client.bucket_exists(artifact_bucket):
                minio_client.make_bucket(artifact_bucket)
                logger.info(f"Created MinIO bucket: {artifact_bucket}")
            model_path = f"/mlflow/mlruns/{run.info.experiment_id}/{run.info.run_id}/artifacts/model"
            for root, _, files in os.walk(model_path):
                for file in files:
                    local_path = os.path.join(root, file)
                    minio_path = f"{run.info.run_id}/{os.path.relpath(local_path, model_path)}"
                    minio_client.fput_object(artifact_bucket, minio_path, local_path)
            logger.info(f"Saved to MinIO: {artifact_bucket}/{run.info.run_id}")

            logger.info(f"Model logged with run_id: {run.info.run_id}")