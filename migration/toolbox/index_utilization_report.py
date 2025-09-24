import json  
import sys  
import os  
  
def list_index_utilization(input_data):  
    found_any = False  
    for doc in input_data:  
        try:  
            # This matches the structure for index_stats in your data  
            if (  
                doc.get('section') == 'data_info'  
                and doc.get('subsection') == 'index_stats'  
                and 'output' in doc  
                and 'cursor' in doc['output']  
                and 'firstBatch' in doc['output']['cursor']  
            ):  
                ns = doc['output']['cursor'].get('ns', 'UNKNOWN')  
                print(f"\nCollection: {ns}")  
                print("-" * (len(ns) + 12))  
                for idx_entry in doc['output']['cursor']['firstBatch']:  
                    key = idx_entry.get('key')  
                    stats = idx_entry.get('stats', [])  
                    if stats:  
                        accesses = stats[0].get('accesses')  
                        if isinstance(accesses, dict) and '$numberLong' in accesses:  
                            accesses = int(accesses['$numberLong'])  
                    else:  
                        accesses = 0  
                    print(f"Index: {json.dumps(key)}  ->  {accesses} accesses")  
                found_any = True  
        except Exception:  
            continue  
    if not found_any:  
        print("No index usage data found in the file. Is this the correct file?")  
  
if __name__ == "__main__":  
    if len(sys.argv) != 2:  
        print("Usage:")  
        print("  python index_utilization_report.py <filename.json>")  
        sys.exit(1)  
  
    filename = sys.argv[1]  
    if not os.path.isfile(filename):  
        print(f"File '{filename}' does not exist.")  
        sys.exit(1)  
  
    try:  
        with open(filename, "r") as f:  
            data = json.load(f)  
    except json.JSONDecodeError:  
        print(f"Could not parse '{filename}' as JSON.")  
        sys.exit(1)  
  
    list_index_utilization(data)  

