import os


from main import ask_ollama


etl_steps = [
    "Read yellow_tripdata_2022-01.parquet into a Spark DataFrame",
    "Filter out records with passenger_count <= 0 or trip_distance <= 0",
    "Compute a new column fare_per_mile = fare_amount / trip_distance",
    "Group by passenger_count and compute average fare_per_mile",
    "Write the result to CSV and Parquet in the 'output/' directory"
]

readme_path = "README.md"

with open(readme_path, "w", encoding="utf-8") as f:
    f.write("# 🚖 Intelligent Data Pipeline for NYC Yellow Taxi Data\n\n")
    f.write("This documentation was generated using a local LLM via Ollama.\n\n")
    f.write("Below is a step-by-step explanation of the data pipeline implemented in `pipeline.py`.\n\n")

    for i, step in enumerate(etl_steps, start=1):
        prompt = f"Explain step {i} in an ETL pipeline: {step}. Write it clearly like you're documenting code for a README."
        explanation = ask_ollama(prompt)
        section = f"## Step {i}: {step}\n\n{explanation.strip()}\n\n"
        print(section)
        f.write(section)

    f.write("---\n")
    f.write("### 📁 Output\n")
    f.write("- `output/aggregated_results.parquet` – Cleaned and aggregated data\n")
    f.write("- `output/output.csv` – Same data in CSV format for dashboard use\n\n")

    f.write("### 📊 Dashboard\n")
    f.write("- Built with **Streamlit** and **Plotly**\n")
    f.write("- Located in `streamlit_app.py`\n")
    f.write("- Auto-refresh supported for data updates (if implemented)\n\n")

    f.write("### 🚀 How to Run\n")
    f.write("```bash\npython pipeline.py\nstreamlit run streamlit_app.py\n```\n")

print(f"\n✅ Documentation saved to {readme_path}")
