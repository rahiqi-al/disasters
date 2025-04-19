import streamlit as st
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from PIL import Image
import io
import mlflow
from mlflow.tracking import MlflowClient
import numpy as np
import pandas as pd
import tensorflow as tf
import pickle
import sklearn

# --- Page Setup ---
st.set_page_config(
    page_title="Wildfire Risk Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Auto-Refresh Every 65 Minutes ---
st.markdown("""
<script>
setTimeout(function() {
    window.location.reload();
}, 3900000);
</script>
""", unsafe_allow_html=True)

# --- Custom CSS Styling (Dark Theme) ---
st.markdown("""
<style>
body {
    background-color: #1e1e2f;
    font-family: 'Verdana', sans-serif;
    color: #d1d1e6;
}
.main-container {
    max-width: 1000px;
    margin: 0 auto;
    padding: 20px;
}
.card {
    background-color: #2a2a3d;
    border-radius: 10px;
    padding: 20px;
    margin: 10px 0;
    box-shadow: 0 4px 8px rgba(0,0,0,0.2);
}
.weather-item {
    display: flex;
    justify-content: space-between;
    padding: 10px 0;
    font-size: 1.3rem;
    border-bottom: 1px solid #3b3b4f;
}
.weather-label {
    font-weight: 600;
    color: #a9a9c2;
}
.weather-value {
    font-weight: 500;
    color: #e0e0f5;
}
.fwi-value {
    font-size: 3.5rem;
    font-weight: bold;
    text-align: center;
    margin: 15px 0;
}
.fwi-risk {
    font-size: 1.5rem;
    font-weight: 600;
    text-align: center;
    margin-bottom: 10px;
}
.fwi-meaning {
    font-size: 1.2rem;
    color: #b0b0c8;
    text-align: center;
}
.upload-box {
    border: 3px dashed #4a4a6a;
    border-radius: 10px;
    padding: 30px;
    text-align: center;
    background-color: #35354f;
    margin: 15px 0;
    transition: border-color 0.3s ease;
}
.upload-box:hover {
    border-color: #1e90ff;
}
.stExpander {
    background-color: #2a2a3d;
    border-radius: 10px;
    border: 1px solid #3b3b4f;
}
.stExpander > div > div {
    color: #d1d1e6 !important;
    font-weight: 600;
    font-size: 1.2rem;
}
.stExpander > div > div:hover {
    background-color: #3b3b4f;
}
h1 {
    color: #e0e0f5;
    font-weight: 700;
}
.stButton > button {
    background-color: #1e90ff;
    color: #ffffff;
    border-radius: 8px;
    padding: 10px 20px;
    font-weight: 600;
}
.stButton > button:hover {
    background-color: #1c86ee;
}
</style>
""", unsafe_allow_html=True)

# --- MLflow Setup ---
mlflow.set_tracking_uri("file:/mlflow/mlruns")

# --- Cassandra Connection ---
def connect_to_cassandra():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['cassandra'], port=9042, auth_provider=auth_provider)
        session = cluster.connect('weather_keyspace')
        return session
    except Exception as e:
        st.error(f"Cassandra connection failed: {str(e)}")
        return None

# --- Fetch Weather Data ---
def fetch_weather_data(session):
    if not session:
        return None
    try:
        query = """SELECT temperature, rh, ws, rain, ffmc, dmc, dc, isi, bui, ingestion_time 
                   FROM weather_keyspace.weather_data LIMIT 1"""
        row = session.execute(query).one()
        if row:
            return {
                "Temperature": row.temperature,
                "RH": row.rh,
                "Ws": row.ws,
                "Rain": row.rain,
                "FFMC": row.ffmc,
                "DMC": row.dmc,
                "DC": row.dc,
                "ISI": row.isi,
                "BUI": row.bui,
                "ingestion_time": row.ingestion_time.strftime("%Y-%m-%d %H:%M:%S")
            }
        st.warning("No weather data found in Cassandra.")
        return None
    except Exception as e:
        st.error(f"Error fetching weather data: {str(e)}")
        return None

# --- Load Best Models and Scaler ---
def load_best_model_and_scaler(experiment_name, metric, order="DESC"):
    client = MlflowClient()
    experiments = client.search_experiments(filter_string=f"name='{experiment_name}'")
    if not experiments:
        st.error(f"No experiment named {experiment_name} found.")
        return None, None
    experiment_id = experiments[0].experiment_id
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        order_by=[f"metrics.{metric} {order}"],
        max_results=1
    )
    if not runs:
        st.error(f"No runs found for experiment {experiment_name}.")
        return None, None
    best_run = runs[0]
    try:
        if experiment_name == "cnn_fire_prediction":
            model = mlflow.keras.load_model(f"runs:/{best_run.info.run_id}/model")
            scaler = None
        else:
            model = mlflow.sklearn.load_model(f"runs:/{best_run.info.run_id}/model")
            scaler_path = f"/mlflow/mlruns/{experiment_id}/{best_run.info.run_id}/artifacts/scaler/scaler.pkl"
            with open(scaler_path, "rb") as f:
                scaler = pickle.load(f)
        return model, scaler
    except Exception as e:
        st.error(f"Error loading model/scaler: {str(e)}")
        return None, None

# --- FWI Prediction ---
def predict_fwi(model, scaler, weather_data):
    if not model or not scaler or not weather_data:
        return None, None, None, None
    try:
        input_data = np.array([[weather_data["Temperature"], weather_data["RH"], 
                               weather_data["Ws"], weather_data["Rain"],
                               weather_data["FFMC"], weather_data["DMC"],
                               weather_data["DC"], weather_data["ISI"],
                               weather_data["BUI"]]])
        input_scaled = scaler.transform(input_data)
        fwi = model.predict(input_scaled)[0]
        if isinstance(fwi, np.ndarray):
            fwi = fwi.item()
        if fwi < 5:
            risk, color, meaning = "Very Low", "#27ae60", "Little to no fire risk"
        elif 5 <= fwi < 10:
            risk, color, meaning = "Low", "#1e90ff", "Minimal fire danger, small fires possible"
        elif 10 <= fwi < 20:
            risk, color, meaning = "Moderate", "#f1c40f", "Fires can start and spread moderately"
        elif 20 <= fwi < 30:
            risk, color, meaning = "High", "#e67e22", "High fire risk, fires spread quickly"
        else:
            risk, color, meaning = "Very High/Extreme", "#c0392b", "Severe fire danger, rapid spread"
        return fwi, risk, meaning, color
    except Exception as e:
        st.error(f"Error predicting FWI: {str(e)}")
        return None, None, None, None

# --- Image Prediction ---
def predict_image(model, img_file):
    if not model or not img_file:
        return None
    try:
        img = Image.open(img_file).convert('RGB')
        img = img.resize((256, 256))  # Match CNN input size
        img_array = np.array(img) / 255.0
        img_array = np.expand_dims(img_array, axis=0)
        prediction = model.predict(img_array)[0]
        if prediction > 0.5:
            return "No Fire"
        return "Fire"
    except Exception as e:
        st.error(f"Error predicting image: {str(e)}")
        return None

# --- Main Dashboard ---
st.title("Wildfire Risk Monitoring System")
st.markdown('<div class="main-container">', unsafe_allow_html=True)

# Load Models and Scaler
regression_model, scaler = load_best_model_and_scaler("fire_weather_index_prediction", "rmse", "ASC")
cnn_model, _ = load_best_model_and_scaler("cnn_fire_prediction", "accuracy", "DESC")

# Weather Data
with st.expander("Weather Data", expanded=True):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    session = connect_to_cassandra()
    weather_data = fetch_weather_data(session)
    if weather_data:
        st.markdown(f"""
            <div class="weather-item">
                <span class="weather-label">🌡️ Temperature:</span>
                <span class="weather-value">{weather_data['Temperature']:.1f}°C</span>
            </div>
            <div class="weather-item">
                <span class="weather-label">💧 RH:</span>
                <span class="weather-value">{weather_data['RH']}%</span>
            </div>
            <div class="weather-item">
                <span class="weather-label">🌬️ Wind Speed:</span>
                <span class="weather-value">{weather_data['Ws']:.1f} km/h</span>
            </div>
            <div class="weather-item">
                <span class="weather-label">🌧️ Rain:</span>
                <span class="weather-value">{weather_data['Rain']:.1f} mm</span>
            </div>
            <div class="weather-item">
                <span class="weather-label">🕒 Last updated:</span>
                <span class="weather-value">{weather_data['ingestion_time']}</span>
            </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# FWI Prediction
with st.expander("FWI Prediction"):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    if weather_data and regression_model and scaler:
        fwi, risk, meaning, color = predict_fwi(regression_model, scaler, weather_data)
        if fwi is not None:
            st.markdown(f"""
                <div class="fwi-value" style="color: {color};">{fwi:.1f}</div>
                <div class="fwi-risk" style="color: {color};">{risk} Risk</div>
                <div class="fwi-meaning">{meaning}</div>
            """, unsafe_allow_html=True)
    else:
        st.warning("Unable to display FWI prediction. Check Cassandra connection or model loading.")
    st.markdown('</div>', unsafe_allow_html=True)

# Image Analysis
with st.expander("Image Analysis"):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="upload-box">', unsafe_allow_html=True)
    upload_option = st.radio("Image Source:", ("Upload Image", "Take Photo"), horizontal=True)
    img_file = None
    if upload_option == "Upload Image":
        img_file = st.file_uploader("Choose wildfire-related image", type=["jpg", "png", "jpeg"], label_visibility="collapsed")
    else:
        img_file = st.camera_input("Take a picture of fire conditions", label_visibility="collapsed")
    st.markdown('</div>', unsafe_allow_html=True)
    
    if img_file:
        st.image(img_file, caption="Uploaded Image", width=400)
        if st.button("Analyze Image", type="primary"):
            with st.spinner("Classifying..."):
                prediction = predict_image(cnn_model, img_file)
                if prediction:
                    st.success(f"Classification Result : {prediction}")
                    if prediction == "Fire":
                        st.error("🚨 Fire detected! High danger! 🚨")
                    else:
                        st.info("No fire detected.")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# --- Sidebar Note ---
st.sidebar.markdown("""
### Dashboard Info
The Fire Weather Index (FWI) measures fire danger, indicating the likelihood of forest fires based on weather conditions. It uses:
- **Temperature**
- **Relative Humidity (RH)**: 21–90%
- **Wind Speed (Ws)**: 6–29 km/h
- **Rain**:  0–16.8 mm
- **FFMC**: Fine Fuel Moisture Code, 28.6–92.5
- **DMC**: Duff Moisture Code, 1.1–65.9
- **DC**: Drought Code, 7–220.4
- **ISI**: Initial Spread Index, 0–18.5
- **BUI**: Buildup Index, 1.1–68

**FWI Ranges**:
- **0–5**: Very Low (little to no fire risk)
- **5–10**: Low (minimal danger, small fires possible)
- **10–20**: Moderate (fires start and spread moderately)
- **20–30**: High (high risk, fires spread quickly)
- **30+**: Very High/Extreme (severe danger, rapid spread)
""")