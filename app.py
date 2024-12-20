import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox
import threading
import os
import shutil
import time
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("SparkHealth") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
    .config("spark.hadoop.io.file.buffer.size", "65536") \
    .config("spark.sql.warehouse.dir", "./spark-warehouse") \
    .getOrCreate()

UPLOAD_FOLDER = "./uploads"
OUTPUT_FOLDER = "./processed"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


selected_file = None
processed_data = None  
log_summary = ""
start_time = None  


def process_file(file_path):
    """Data cleaning pipeline with Spark for the entire dataset"""
    global log_summary, processed_data, start_time
    try:
        start_time = time.time()  
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        fill_values = {col: "NA" for col in df.columns if df.schema[col].dataType.typeName() == "string"}
        df_cleaned = df.dropDuplicates().fillna(fill_values)

        
        processed_data = df_cleaned

        
        elapsed_time = time.time() - start_time
        log_summary = (
            f"Rows before cleaning: {df.count()}\n"
            f"Rows after cleaning: {df_cleaned.count()}\n"
            f"Columns processed: {len(df.columns)}\n"
            f"Missing values filled: {sum([df.filter(df[col].isNull()).count() for col in fill_values])}\n"
            f"Time elapsed: {elapsed_time:.2f} seconds\n"
        )
    except Exception as e:
        log_summary = f"Error processing file: {e}"
        raise


def select_file():
    global selected_file
    selected_file = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    if selected_file:
        lbl_selected_file.config(text=f"Selected: {selected_file}")
    else:
        lbl_selected_file.config(text="No file selected")


def update_progress(progress, elapsed_time):
    """Update the progress bar and show elapsed time"""
    progress_bar["value"] = progress
    progress_percentage.config(text=f"{progress:.0f}%")
    elapsed_time_label.config(text=f"Time Elapsed: {elapsed_time:.2f} seconds")
    root.update_idletasks()


def process_file_thread():
    global processed_data
    if not selected_file:
        messagebox.showerror("Error", "Please select a file first")
        return

    def task():
        try:
            progress_bar.start()
            process_file(selected_file)  

            
            for i in range(1, 101):
                time.sleep(0.05)  
                elapsed_time = time.time() - start_time
                update_progress(i, elapsed_time)

            progress_bar.stop()
            messagebox.showinfo("Success", "File processed successfully")
        except Exception as e:
            progress_bar.stop()
            messagebox.showerror("Error", str(e))

    threading.Thread(target=task).start()


def download_file():
    if processed_data is None:
        messagebox.showerror("Error", "No processed data available")
        return

    
    save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv")])
    if save_path:
        try:
            
            processed_data.coalesce(1).write.mode("overwrite").csv(save_path, header=True)
            messagebox.showinfo("Success", f"File saved to {save_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Error saving file: {e}")


def view_log():
    if not log_summary:
        messagebox.showinfo("Log", "No log available")
        return

    log_window = ttk.Toplevel(root)
    log_window.title("Process Log")

    text_log = ttk.Text(log_window, wrap="word", width=80, height=20)
    text_log.insert("end", log_summary)
    text_log.config(state="disabled")
    text_log.pack(padx=10, pady=10)



root = ttk.Window(themename="superhero")
root.title("Spark Health")
root.geometry("500x400")


header = ttk.Label(root, text="Spark Health Tool", font=("Helvetica", 18, "bold"), anchor="center")
header.pack(pady=20)


btn_select = ttk.Button(root, text="Select File", command=select_file, bootstyle="primary")  
btn_select.pack(pady=10)

lbl_selected_file = ttk.Label(root, text="No file selected", wraplength=400)
lbl_selected_file.pack()


btn_process = ttk.Button(root, text="Process", command=process_file_thread, bootstyle="success")  
btn_process.pack(pady=10)

progress_bar = ttk.Progressbar(root, orient="horizontal", mode="determinate", length=300, bootstyle="info")  
progress_bar.pack(pady=10)

progress_percentage = ttk.Label(root, text="0%", font=("Helvetica", 12))
progress_percentage.pack()

elapsed_time_label = ttk.Label(root, text="Time Elapsed: 0.00 seconds", font=("Helvetica", 12))
elapsed_time_label.pack()


btn_download = ttk.Button(root, text="Download Processed File", command=download_file, bootstyle="warning")  
btn_download.pack(pady=10)


btn_view_log = ttk.Button(root, text="View Process Log", command=view_log, bootstyle="secondary")  
btn_view_log.pack(pady=10)

root.mainloop()
