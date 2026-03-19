import json
import os
import requests
import sseclient
import threading
import queue
import time
from datetime import datetime

# Core Components
from core.normalizer import Normalizer
from core.analyzer import Analyzer
from core.classifier import Classifier
from core.query_engine import QueryEngine
from core.router import Router
from core.metadata_manager import MetadataManager

# DB Handlers
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler
from dotenv import load_dotenv

# Configuration
BATCH_SIZE = 50
DATA_STREAM_URL = "http://127.0.0.1:8000/record/5000"
MAX_QUEUE_SIZE = 1000
STOP_EVENT = threading.Event()

# --- WORKER FUNCTIONS ---

def ingest_worker(raw_queue, data_url):
    """Fetches data from stream and puts into Raw Queue."""
    pre_processor = Normalizer()
    record_count = 0
    
    try:
        try:
            requests.get(data_url.replace('/record/5000', '/'), timeout=2)
        except:
            print("⚠️  Simulation Server unreachable. Retrying...")
            time.sleep(2)

        response = requests.get(data_url, stream=True, timeout=30)
        client = sseclient.SSEClient(response)
        
        event_count = 0
        try:
            for event in client.events():
                event_count += 1
                if STOP_EVENT.is_set():
                    
                    break
                
             
                if event.data:
                    try:
                        
                        raw_record = json.loads(event.data)
                        clean_record = pre_processor.normalize_record(raw_record)
                        
                        record_count += 1
                        
                        
                        if record_count % 100 == 0:
                            print(f"[Ingestor] Received {record_count} records")
                        
                        try:
                            
                            raw_queue.put(clean_record, timeout=1)
                            
                        except queue.Full:
                            pass
                            
                    except json.JSONDecodeError as e:
                        continue
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        continue
                else:
                    print(f"[Ingestor] NO DATA in event {event_count}, skipping")
                
        except Exception as e:
            print(f"[Ingestor] EXCEPTION in event loop: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        print(f"[Ingestor] Event loop ended after {event_count} events, {record_count} records processed")
    except Exception as e:
        print(f"[Ingestor] Stream Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    print("[Ingestor] Stopped.")

def process_worker(raw_queue, write_queue, analyzer, classifier):
    """Analyzes batches of data."""
    buffer = []
    last_dispatch = time.time()
    timeout = 2  # seconds
    while not STOP_EVENT.is_set():
        try:
            record = raw_queue.get(timeout=1)
            buffer.append(record)
            raw_queue.task_done()
        except queue.Empty:
            pass

        # Only dispatch if buffer is full or timeout
        now = time.time()
        if len(buffer) >= BATCH_SIZE or (len(buffer) > 0 and (now - last_dispatch) > timeout):
            try:
                analyzer.analyze_batch(buffer)
                stats = analyzer.get_schema_stats()
                schema_decisions = classifier.decide_schema(stats)
                payload = {
                    "batch": buffer,
                    "decisions": schema_decisions
                }
                write_queue.put(payload)
            except Exception as e:
                print(f"[Processor] Error: {e}")
            buffer = []
            last_dispatch = now

def router_worker(write_queue, router, analyzer):
    print("[Router] Worker started.")
    
    last_save_time = time.time()
    
    while not STOP_EVENT.is_set():
        try:
            payload = write_queue.get(timeout=1)
            batch = payload['batch']
            decisions = payload['decisions']
            
            # --- EXECUTE ROUTING ---
            router.process_batch(batch, decisions)
            
            # Update analyzer's field_stats with db assignments from router
            analyzer.update_db_assignment(decisions)
            
            full_metadata = {
                "analyzer": analyzer.export_stats(),
                # "classifier_decisions": payload.get('classifier_decisions', {}),
                # "router_decisions": router.export_decisions()
            }
            save_metadata(full_metadata)
            
            write_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"[Router] Error: {e}")

    # Final Save
    meta_manager.sync_analyzer(analyzer)
    meta_manager.sync_router(router)
    meta_manager.save_metadata()
    print("[Router] Stopped.")

# --- MAIN EXECUTION ---

def main():
    load_dotenv()
    
    print("="*60)
    print("  ADAPTIVE INGESTION ENGINE v2.0 (Metadata Driven)")
    print("="*60)
    
    # 1. Initialize Metadata Manager
    meta_manager = MetadataManager()
    print(f"[Init] Metadata Manager loaded.")

    # 2. Initialize Queues
    raw_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
    write_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
    
    # 3. Initialize Core Components
    analyzer = Analyzer()
    # Restore analyzer state from previous runs
    meta_manager.restore_analyzer_state(analyzer)
    classifier = Classifier(lower_threshold=0.75, upper_threshold=0.85)
    
    sql_handler = SQLHandler()
    mongo_handler = MongoHandler()
    
    router = Router(sql_handler, mongo_handler, analyzer)
    
    print("[Init] Components initialized.")

    # 4. Start Threads
    t_ingest = threading.Thread(target=ingest_worker, args=(raw_queue, DATA_STREAM_URL))
    t_process = threading.Thread(target=process_worker, args=(raw_queue, write_queue, analyzer, classifier))
    t_router = threading.Thread(target=router_worker, args=(write_queue, router, analyzer))

    t_ingest.start()
    t_process.start()
    t_router.start()

    # Initialize Query Engine for CLI
    query_engine = QueryEngine(analyzer, raw_queue)
    
    print("\n" + "="*60)
    print("  SYSTEM READY")
    print("="*60)
    print("\nAvailable Commands:")
    print("  • status           - Show system uptime and processing statistics")
    print("  • stats <field>    - Display detailed analysis for a specific field")
    print("  • all_stats        - View statistics for all tracked fields")
    print("  • queue            - Check current queue sizes")
    print("  • help             - Show detailed command help")
    print("  • exit             - Shut down the system gracefully\n")
    
    try:
        while True:
            try:
                command = input(">> ")
                if command.lower() == "exit":
                    print("\nInitiating shutdown...")
                    break
                response = query_engine.process_command(command)
                if response:
                    print(response)
            except EOFError:
                break
            except Exception as e:
                print(f"Error processing command: {e}")
    except KeyboardInterrupt:
        print("\n⚠️  Interrupt received.\nStopping worker threads...")
        
    print("[Ingestor] Thread stopping.")
    print("[Router] Thread stopping.")
    print("\n[System] Shutting down...")
    STOP_EVENT.set()
    
    t_ingest.join(timeout=2)
    t_process.join(timeout=2)
    t_router.join(timeout=2)
    
    print("[System] Done.")

if __name__ == "__main__":
    main()