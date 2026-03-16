import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
from plotly.subplots import make_subplots
from flask import request, render_template, jsonify
import json
import logging
import textwrap
import requests
from datetime import datetime, timezone
from bson import Timestamp
from pymongo.errors import PyMongoError
from mongosync_plot_utils import format_byte_size, convert_bytes


def get_phase_timestamp(phase_transitions, phase_name):
    """Find the first matching phase and return its timestamp as datetime."""
    if not phase_transitions:
        return None
    for pt in phase_transitions:
        if pt.get("phase") == phase_name:
            ts = pt.get("ts")
            if isinstance(ts, Timestamp):
                return datetime.fromtimestamp(ts.time, tz=timezone.utc)
            elif isinstance(ts, datetime):
                return ts
    return None

def gatherMetrics(connection_string, endpoint_url=None):
    # Use the centralized logging and configuration
    logger = logging.getLogger(__name__)
    
    # Import and use the centralized configuration
    from app_config import INTERNAL_DB_NAME, get_database
    
    TARGET_MONGO_URI = connection_string
    internalDb = INTERNAL_DB_NAME
    
    # Connect to MongoDB cluster using connection pooling
    try:
        internalDbDst = get_database(TARGET_MONGO_URI, internalDb)
        logger.info("Connected to target MongoDB cluster using connection pooling.")
    except PyMongoError as e:
        logger.error(f"Failed to connect to target MongoDB: {e}")
        return jsonify({
            "error": "Cannot connect to MongoDB cluster",
            "message": "Failed to connect to the MongoDB cluster. Please verify the connection string, network connectivity, and cluster availability.",
            "error_type": "connection_failed",
            "details": str(e)
        }), 503
    # Create a subplot for status information (5 rows x 2 cols, chart in col 1, tables in col 2)
    fig = make_subplots(rows=5, 
                        cols=2, 
                        row_heights=[0.20, 0.20, 0.20, 0.20, 0.20],
                        column_widths=[0.15, 0.30],
                        specs=[[{"type": "xy", "rowspan": 5}, {"type": "table"}],
                               [None, {"type": "table"}],
                               [None, {"type": "table"}],
                               [None, {"type": "table"}],
                               [None, {"type": "table"}]],
                        subplot_titles=("Data Copy Progress", ""),
                        vertical_spacing=0.002,
                        horizontal_spacing=0.10
                        )

    #Get State and Phase from resumeData collection
    vResumeData = internalDbDst.resumeData.find_one({"_id": "coordinator"})
    
    # When mongosync is IDLE (no resume data), return friendly message instead of chart
    if vResumeData is None:
        # Verify state from progress API to distinguish: running+IDLE vs not running
        state_from_endpoint = None
        endpoint_reachable = False
        if endpoint_url:
            try:
                response = requests.get(f"http://{endpoint_url}", timeout=10)
                if response.ok:
                    data = response.json()
                    state_from_endpoint = data.get("progress", {}).get("state")
                    endpoint_reachable = True
            except Exception:
                pass
        if not endpoint_reachable:
            # Mongosync process is not running (progress API unreachable)
            return jsonify({
                "error_type": "not_running",
                "message": "Mongosync is not running. Please launch it using the Launch button on the left menu bar."
            })
        if state_from_endpoint is None or state_from_endpoint == "IDLE":
            # Mongosync is running and in IDLE state
            return jsonify({
                "error_type": "idle_state",
                "message": "Mongosync is in IDLE state. Please start the migration using the Start button in the left sidebar."
            })
    
    # Fetch Source, Destination, Can Commit, Can Write, Mongosync ID, Coordinator ID from endpoint if available
    sourceText = "No Data"
    destinationText = "No Data"
    canCommitValue = "No Data"
    canWriteValue = "No Data"
    canCommitColor = "black"
    canWriteColor = "black"
    mongosyncIDValue = "No Data"
    coordinatorIDValue = "No Data"
    
    if endpoint_url:
        try:
            response = requests.get(f"http://{endpoint_url}", timeout=10)
            response.raise_for_status()
            data = response.json()
            progress_data = data.get("progress", {})
            
            # Extract source and destination from directionMapping
            directionMapping = progress_data.get("directionMapping", {})
            if directionMapping and isinstance(directionMapping, dict):
                sourceText = directionMapping.get("Source", "No Data")
                destinationText = directionMapping.get("Destination", "No Data")
            
            # Extract canCommit and canWrite
            canCommit = progress_data.get("canCommit")
            if canCommit is not None:
                canCommitValue = str(canCommit).capitalize()
                canCommitColor = "green" if canCommit else "red"
            
            canWrite = progress_data.get("canWrite")
            if canWrite is not None:
                canWriteValue = str(canWrite).capitalize()
                canWriteColor = "green" if canWrite else "red"
            
            # Extract mongosyncID and coordinatorID
            mongosyncID = progress_data.get("mongosyncID")
            if mongosyncID:
                mongosyncIDValue = str(mongosyncID).capitalize()
            
            coordinatorID = progress_data.get("coordinatorID")
            if coordinatorID:
                coordinatorIDValue = str(coordinatorID).capitalize()
                
        except Exception as e:
            logger.warning(f"Could not fetch data from endpoint: {e}")
    
    # Row 1: Source and Destination (combined in one cell)
    fig.add_trace(go.Table(
        header=dict(values=[], height=0, line_width=0),
        cells=dict(
            values=[['<b>Source</b>', '<b>Destination</b>'], 
                    [sourceText, destinationText]],
            align=['left', 'left'],
            font=dict(size=12),
            height=36,
            line_width=0.6,
            line_color="#D9E0E3",
            fill_color='white',
        ),
        columnwidth=[70, 196]
    ), row=1, col=2)

    #Plot mongosync State
    vState = vResumeData["state"] if vResumeData and "state" in vResumeData else "IDLE"
    if vState == 'RUNNING':
        vColor = 'white'
        vBgColor = '#00A35C'  # MongoDB green
    elif vState == "IDLE":
        vColor = 'black'
        vBgColor = "#FFC010"  # MongoDB yellow
    elif vState == "PAUSED":
        vColor = 'black'
        vBgColor = "#FFC010"  # MongoDB yellow
    elif vState == "COMMITTING":
        vColor = 'black'
        vBgColor = "#90EE90"  # Light green
    elif vState == "COMMITTED":
        vColor = 'white'
        vBgColor = '#2196F3'  # Blue
    else:
        logging.warning(vState + " is not listed as an option")
        vColor = 'white'
        vBgColor = "#89979B"  # MongoDB gray
    
    # Row 2: Current State and Current Phase (combined in one cell)
    vPhase = vResumeData.get("syncPhase", "N/A").capitalize() if vResumeData else "N/A"
    fig.add_trace(go.Table(
        header=dict(values=[], height=0, line_width=0),
        cells=dict(
            values=[['<b>Current State</b>', '<b>Current Phase</b>'], 
                    [f'<b>{str(vState)}</b>', str(vPhase)]],
            align=['left', 'left'],
            font=dict(size=12, color=[['black', 'black'], [vColor, 'black']]),
            height=36,
            line_width=0.6,
            line_color="#D9E0E3",
            fill_color=[['white', 'white'], [vBgColor, 'white']],
        ),
        columnwidth=[70, 196]
    ), row=2, col=2)

    # Row 3: Can Commit and Can Write (combined in one cell)
    fig.add_trace(go.Table(
        header=dict(values=[], height=0, line_width=0),
        cells=dict(
            values=[['<b>Can Commit</b>', '<b>Can Write</b>'], 
                    [canCommitValue, canWriteValue]],
            align=['left', 'left'],
            font=dict(size=12, color=[['black', 'black'], [canCommitColor, canWriteColor]]),
            height=36,
            line_width=0.6,
            line_color="#D9E0E3",
            fill_color='white',
        ),
        columnwidth=[70, 196]
    ), row=3, col=2)

    #Plot Lag Time (fetched from mongosync endpoint)
    def format_lag_time(seconds):
        """Format lag time in seconds to human-readable format."""
        if seconds is None:
            return "No Data"
        try:
            total_seconds = int(seconds)
            if total_seconds < 0:
                return "0s"
            days, remainder = divmod(total_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, secs = divmod(remainder, 60)
            parts = []
            if days > 0:
                parts.append(f"{days}d")
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)
        except (ValueError, TypeError):
            return "No Data"
    
    # Fetch lag time from endpoint if available
    lagTimeText = 'No Data'
    if endpoint_url:
        try:
            response = requests.get(f"http://{endpoint_url}", timeout=10)
            response.raise_for_status()
            data = response.json()
            lagTimeSeconds = data.get("progress", {}).get("lagTimeSeconds")
            if lagTimeSeconds is not None:
                lagTimeText = format_lag_time(lagTimeSeconds)
        except Exception as e:
            logger.warning(f"Could not fetch lag time from endpoint: {e}")
            lagTimeText = "No Data"

    # Row 4: Lag Time and Start / Finish (combined in one cell)
    phaseTransitions = vResumeData.get("phaseTransitions", []) if vResumeData else []
    newInitial = get_phase_timestamp(phaseTransitions, "initializing collections and indexes")
    if newInitial is None:
        newInitialText = 'NO DATA'
    else:
        newInitialText = newInitial.strftime("%Y-%m-%d %H:%M:%S")
    
    newFinish = get_phase_timestamp(phaseTransitions, "commit completed")
    if newFinish is None:
        newFinishText = 'NO DATA'
    else:
        newFinishText = newFinish.strftime("%Y-%m-%d %H:%M:%S")
    
    # Combine Start and Finish as "Start / Finish: <start time> / <finish time>"
    startFinishText = f"{newInitialText} / {newFinishText}"
    
    fig.add_trace(go.Table(
        header=dict(values=[], height=0, line_width=0),
        cells=dict(
            values=[['<b>Lag Time</b>', '<b>Start / Finish</b>'], 
                    [lagTimeText, startFinishText]],
            align=['left', 'left'],
            font=dict(size=12),
            height=36,
            line_width=0.6,
            line_color="#D9E0E3",
            fill_color='white',
        ),
        columnwidth=[70, 196]
    ), row=4, col=2)
    
    # Row 5: Mongosync ID and Coordinator ID (combined in one cell)
    fig.add_trace(go.Table(
        header=dict(values=[], height=0, line_width=0),
        cells=dict(
            values=[['<b>Mongosync ID</b>', '<b>Coordinator ID</b>'], 
                    [mongosyncIDValue, coordinatorIDValue]],
            align=['left', 'left'],
            font=dict(size=12),
            height=36,
            line_width=0.6,
            line_color="#D9E0E3",
            fill_color='white',
        ),
        columnwidth=[70, 196]
    ), row=5, col=2)
    
    # Data Copy Progress stacked horizontal bar chart (Column 1, spanning all 5 rows)
    vGroup = {"$group":{"_id": None, "totalCopiedBytes": { "$sum": "$copiedByteCount" }, "totalBytesCount": { "$sum": "$totalByteCount" }  }}
    vCompleteData = internalDbDst.partitions.aggregate([vGroup])
    vCompleteData = list(vCompleteData)
    vCopiedBytes = 0
    vTotalBytes = 0
    
    if len(vCompleteData) == 0:
        fig.add_trace(go.Scatter(
            x=[0],
            y=["Progress"],
            text=["NO DATA"],
            mode='text',
            textfont=dict(size=11, color="black"),
            showlegend=False
        ), row=1, col=1)
        fig.update_xaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1)
        fig.update_yaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1)
    else:        
        for comp in list(vCompleteData):
            vCopiedBytes = comp["totalCopiedBytes"] + vCopiedBytes
            vTotalBytes = comp["totalBytesCount"] + vTotalBytes

        if vTotalBytes <= 0:
            fig.add_trace(go.Scatter(
                x=[0],
                y=["Progress"],
                text=["NO DATA"],
                mode='text',
                textfont=dict(size=11, color="black"),
                showlegend=False
            ), row=1, col=1)
            fig.update_xaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1)
            fig.update_yaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1)
        else:
            vTotalBytesFormatted, estimated_total_bytes_unit = format_byte_size(vTotalBytes)
            vCopiedBytesFormatted = convert_bytes(vCopiedBytes, estimated_total_bytes_unit)
            vRemainingBytesFormatted = max(0, vTotalBytesFormatted - vCopiedBytesFormatted)
            progress_pct = (vCopiedBytes / vTotalBytes) * 100

            fig.add_trace(go.Bar(
                x=[vCopiedBytesFormatted],
                y=["Data Copy"],
                orientation='h',
                marker=dict(color="#00A35C"),
                name="Copied",
                text=[f"{progress_pct:.1f}% complete"],
                textposition='inside',
                textfont=dict(size=11, color='white'),
                hovertemplate=f"Copied: %{{x:.2f}} {estimated_total_bytes_unit}<extra></extra>",
                showlegend=False
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=[vRemainingBytesFormatted],
                y=["Data Copy"],
                orientation='h',
                marker=dict(color="#C1C7C6"),
                name="Remaining",
                text=[f"{vCopiedBytesFormatted:.2f}/{vTotalBytesFormatted:.2f} {estimated_total_bytes_unit}"],
                textposition='outside',
                textfont=dict(size=10, color='#3D4F58'),
                hovertemplate=f"Remaining: %{{x:.2f}} {estimated_total_bytes_unit}<extra></extra>",
                showlegend=False
            ), row=1, col=1)

            fig.update_layout(barmode='stack')
            fig.update_xaxes(
                title_text=f"Data ({estimated_total_bytes_unit})",
                title_font=dict(size=9),
                tickfont=dict(size=8),
                range=[0, vTotalBytesFormatted * 1.12],
                row=1,
                col=1
            )
            fig.update_yaxes(
                tickfont=dict(size=10),
                row=1,
                col=1
            )
    
    # Update subplot title annotations - position "Data Copy Progress" above chart area
    fig.update_annotations(font=dict(size=11))
    if fig.layout.annotations:
        fig.layout.annotations[0].update(
            y=1.0, yanchor='bottom', yref='paper',
            yshift=10
        )
    
    # Update layout - compact spacing between status table rows
    fig.update_layout(
        height=500,
        width=850,
        autosize=True,
        showlegend=False,
        plot_bgcolor="white",
        margin=dict(l=10, r=50, t=70, b=40)
    )
    
    # Convert the figure to JSON and return as Flask response
    plot_json = json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))
    return jsonify(plot_json)


def gatherStartupConfigs(connection_string):
    """Fetch and display mongosync startup configuration."""
    logger = logging.getLogger(__name__)
    
    from app_config import INTERNAL_DB_NAME, get_database
    
    TARGET_MONGO_URI = connection_string
    internalDb = INTERNAL_DB_NAME
    
    # Connect to MongoDB cluster using connection pooling
    try:
        internalDbDst = get_database(TARGET_MONGO_URI, internalDb)
        logger.info("Connected to target MongoDB cluster for startup configs.")
    except PyMongoError as e:
        logger.error(f"Failed to connect to target MongoDB: {e}")
        return jsonify({
            "error": "Cannot connect to MongoDB cluster",
            "message": "Failed to connect to the MongoDB cluster. Please verify the connection string, network connectivity, and cluster availability.",
            "error_type": "connection_failed",
            "details": str(e)
        }), 503
    
    # Create a subplot for namespace filters only (1 row x 2 cols)
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "table"}, {"type": "table"}]],
        horizontal_spacing=0.05
    )
    
    #Get globalState values
    vGlobalState = internalDbDst.globalState.find_one({})
    
    # Helper function to format namespace filter data for table display
    def format_namespace_filter(filter_data, filter_type="inclusion"):
        """Convert namespace filter data to table columns (keys, values).
        
        Args:
            filter_data: The filter data from globalState
            filter_type: "inclusion" or "exclusion" - affects empty state message
        """
        # Handle empty/null filter data
        if not filter_data:
            if filter_type == "inclusion":
                return ["Database"], ["All (no filter)"]
            else:  # exclusion
                return ["Filter"], ["No filter"]

        # Normalize possible filter representations from globalState.
        normalized_rules = []
        if isinstance(filter_data, list):
            normalized_rules = filter_data
        elif isinstance(filter_data, dict):
            if any(k in filter_data for k in ("database", "db", "collections", "databases", "namespaces")):
                normalized_rules = [filter_data]
            else:
                # Fallback: map-like structure {"dbA": ["c1", "c2"], "dbB": []}
                normalized_rules = [{"database": db_name, "collections": colls} for db_name, colls in filter_data.items()]
        else:
            normalized_rules = []

        keys = []
        values = []

        for idx, item in enumerate(normalized_rules):
            if isinstance(item, dict):
                # Shape: {"namespaces": ["db.coll", ...]} or list of namespace dicts
                namespaces = item.get("namespaces")
                if isinstance(namespaces, list) and namespaces:
                    db_to_colls = {}
                    for ns in namespaces:
                        db_name = None
                        coll_name = None
                        if isinstance(ns, str) and "." in ns:
                            db_name, coll_name = ns.split(".", 1)
                        elif isinstance(ns, dict):
                            db_name = ns.get("database") or ns.get("db")
                            coll_name = ns.get("collection") or ns.get("coll")
                        if db_name:
                            db_to_colls.setdefault(str(db_name), [])
                            if coll_name:
                                db_to_colls[str(db_name)].append(str(coll_name))
                    for db_name, coll_list in db_to_colls.items():
                        keys.append("Database")
                        values.append(db_name)
                        keys.append("Collections")
                        values.append(", ".join(coll_list) if coll_list else "All (no filter)")
                    continue

                # Extract database info
                database = item.get("database") or item.get("db")
                databases = item.get("databases")
                if not database and isinstance(databases, list):
                    database = databases
                if database:
                    # Flatten nested lists
                    if isinstance(database, list):
                        db_list = []
                        for db in database:
                            if isinstance(db, list):
                                db_list.extend(db)
                            else:
                                db_list.append(str(db))
                        keys.append("Database")
                        values.append(", ".join(db_list) if db_list else "All (no filter)")
                
                # Extract collections info
                collections = item.get("collections")
                if collections:
                    if isinstance(collections, list):
                        keys.append("Collections")
                        values.append(", ".join([str(c) for c in collections]))
                    else:
                        keys.append("Collections")
                        values.append(str(collections))
                elif collections is None and database:
                    keys.append("Collections")
                    values.append("All (no filter)")
        
        if not keys:
            if filter_type == "inclusion":
                return ["Database"], ["All (no filter)"]
            else:  # exclusion
                return ["Filter"], ["No filter"]
        
        return keys, values
    
    # Parse namespaceFilter from globalState
    namespaceFilter = vGlobalState.get("namespaceFilter", {}) if vGlobalState else {}
    inclusionFilter = namespaceFilter.get("inclusionFilter") if namespaceFilter else None
    exclusionFilter = namespaceFilter.get("exclusionFilter") if namespaceFilter else None
    
    # Create Inclusion Filter table (Row 1, Col 1)
    inc_keys, inc_values = format_namespace_filter(inclusionFilter, "inclusion")
    fig.add_trace(go.Table(
        header=dict(
            values=[],
            font=dict(size=12, color='black'),
            align='center',
            height=0,
            line_width=0,
            fill_color='white'
        ),
        cells=dict(values=[inc_keys, inc_values], align=['left'], font=dict(size=10, color='darkblue')),
        columnwidth=[0.75, 2.5]
    ), row=1, col=1)
    
    # Create Exclusion Filter table (Row 1, Col 2)
    exc_keys, exc_values = format_namespace_filter(exclusionFilter, "exclusion")
    fig.add_trace(go.Table(
        header=dict(
            values=[],
            font=dict(size=12, color='black'),
            align='center',
            height=0,
            line_width=0,
            fill_color='white'
        ),
        cells=dict(values=[exc_keys, exc_values], align=['left'], font=dict(size=10, color='darkblue')),
        columnwidth=[0.75, 2.5]
    ), row=1, col=2)

    # Add section titles as full-width annotations above each table to visually span both columns.
    fig.add_annotation(
        x=0.24, y=1.07, xref='paper', yref='paper',
        text='<b>Namespace Filter - Inclusion</b>',
        showarrow=False,
        font=dict(size=12, color='black'),
        xanchor='center',
        yanchor='bottom'
    )
    fig.add_annotation(
        x=0.76, y=1.07, xref='paper', yref='paper',
        text='<b>Namespace Filter - Exclusion</b>',
        showarrow=False,
        font=dict(size=12, color='black'),
        xanchor='center',
        yanchor='bottom'
    )
    
    # Update layout
    fig.update_layout(
        height=320,
        width=900,
        autosize=True,
        showlegend=False,
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=78, b=20)
    )
    
    # Convert the figure to JSON and return as Flask response
    plot_json = json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))
    return jsonify(plot_json)


def gatherPartitionsMetrics(connection_string, endpoint_url=None):
    """Generate progress view with partitions, data copy, phases, and collection progress."""
    logger = logging.getLogger(__name__)
    
    from app_config import INTERNAL_DB_NAME, MAX_PARTITIONS_DISPLAY, get_database
    
    TARGET_MONGO_URI = connection_string
    internalDb = INTERNAL_DB_NAME
    
    try:
        internalDbDst = get_database(TARGET_MONGO_URI, internalDb)
        logger.info("Connected to target MongoDB for progress metrics.")
    except PyMongoError as e:
        logger.error(f"Failed to connect to target MongoDB: {e}")
        return jsonify({
            "error": "Cannot connect to MongoDB cluster",
            "message": "Failed to connect to the MongoDB cluster. Please verify the connection string, network connectivity, and cluster availability.",
            "error_type": "connection_failed",
            "details": str(e)
        }), 503
    
    # Create subplots for progress view (3 rows: 2 cols) - top row uses column charts
    fig = make_subplots(
        rows=3, 
        cols=2, 
        row_heights=[0.50, 0.25, 0.25],
        column_widths=[0.5, 0.5],
        specs=[[{"type": "xy"}, {"type": "xy"}],
               [{"type": "xy"}, {"type": "xy"}],
               [None, None]],
        subplot_titles=(
            "Partitions Completed %",
            "Collection Copy Progress",
            "Mongosync Phases",
            "Collections Progress",
            "",
            ""
        ),
        horizontal_spacing=0.12,
        vertical_spacing=0.08
    )
    
    # Get resumeData for phase transitions
    vResumeData = internalDbDst.resumeData.find_one({"_id": "coordinator"})
    phaseTransitions = vResumeData.get("phaseTransitions", []) if vResumeData else []
    
    # 1. Partitions Completed % (Row 1, Col 1) - column chart
    vGroup1 = {"$group": {"_id": {"namespace": {"$concat": ["$namespace.db", ".", "$namespace.coll"]}, "partitionPhase": "$partitionPhase" },  "documentCount": { "$sum": 1 }}}
    vGroup2 = {"$group": {  "_id": {  "namespace": "$_id.namespace"},  "partitionPhaseCounts": {  "$push": {  "k": "$_id.partitionPhase",  "v": "$documentCount"  }  },  "totalDocumentCount": { "$sum": "$documentCount" }  }  }
    vAddFields1 = {"$addFields": {"namespace": "$_id.namespace"}}
    vProject1 = {"$project": {  "_id": 0,"namespace": 1,"totalDocumentCount": 1,  "partitionPhaseCounts":{"$arrayToObject": "$partitionPhaseCounts" }}  }
    vProject2 = {"$project": {  "_id": 0,"namespace": 1,"totalDocumentCount": 1,  "partitionPhaseCounts": {  "$mergeObjects": [  { "not started": 0, "in progress": 0, "done": 0 },  "$partitionPhaseCounts"  ]  }}  }
    vAddFields2 = {"$addFields": {"PercCompleted": {"$divide": [{ "$multiply": ["$partitionPhaseCounts.done", 100] }, "$totalDocumentCount"]}}}
    vSort1 = {"$sort": {"PercCompleted": 1, "namespace": 1}}  
    vPartitionData = internalDbDst.partitions.aggregate([vGroup1, vGroup2, vAddFields1, vProject1, vProject2, vAddFields2, vSort1])

    vPartitionData = list(vPartitionData)

    # Limits the total of namespaces to MAX_PARTITIONS_DISPLAY in the partitions completed
    if len(vPartitionData) > MAX_PARTITIONS_DISPLAY:  
        # Remove PercCompleted == 100  
        filtered = [doc for doc in vPartitionData if doc.get('PercCompleted') != 100]  
        # If we still have more than MAX_PARTITIONS_DISPLAY, trim to MAX_PARTITIONS_DISPLAY  
        if len(filtered) >= MAX_PARTITIONS_DISPLAY:  
            vPartitionData = filtered[:MAX_PARTITIONS_DISPLAY-1]  
        else:  
            # If after removal less than MAX_PARTITIONS_DISPLAY, fill up with remaining PercCompleted==100  
            needed = MAX_PARTITIONS_DISPLAY - len(filtered)  
            completed_100 = [doc for doc in vPartitionData if doc.get('PercCompleted') == 100]  
            vPartitionData = filtered + completed_100[:needed]  

    if len(vPartitionData) == 0:
        fig.add_trace(go.Scatter(
            x=[0],
            y=[0],
            text="NO DATA",
            mode='text',
            textfont=dict(size=11, color="black"),
            showlegend=False
        ), row=1, col=1)
        fig.update_layout(
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    else:
        vNamespace = []
        vPercComplete = []        
        for partition in vPartitionData:
            vNamespace.append(partition["namespace"])
            vPercComplete.append(partition["PercCompleted"])

        # Threshold buckets: <50 red, 50-80 amber, >80 green
        threshold_colors = []
        for pct in vPercComplete:
            if pct < 50:
                threshold_colors.append("#DB3030")
            elif pct <= 80:
                threshold_colors.append("#FFC010")
            else:
                threshold_colors.append("#00A35C")

        fig.add_trace(go.Bar(
            x=vNamespace,
            y=vPercComplete,
            marker=dict(color=threshold_colors),
            showlegend=False,
            text=[f"{p:.1f}%" for p in vPercComplete],
            textposition='outside',
            textfont=dict(size=9),
            hovertemplate="%{x}<br>% complete: %{y:.1f}%<extra></extra>"
        ), row=1, col=1)
        fig.add_hrect(y0=0, y1=50, fillcolor="#FFEAE5", opacity=0.45, line_width=0, row=1, col=1)
        fig.add_hrect(y0=50, y1=80, fillcolor="#FFF5D6", opacity=0.45, line_width=0, row=1, col=1)
        fig.add_hrect(y0=80, y1=100, fillcolor="#E3FCF7", opacity=0.45, line_width=0, row=1, col=1)
        fig.update_xaxes(tickfont=dict(size=8), tickangle=-20, row=1, col=1)
        fig.update_yaxes(range=[0, 100], ticksuffix='%', tickfont=dict(size=8), row=1, col=1)

    # 2. Mongosync Phases (Row 2, Col 1 - reduced to 50% size)
    def format_phase_label(phase_name):
        """Format phase name with line breaks and abbreviations."""
        phase_lower = phase_name.lower().strip()
        
        # Specific phase formatting rules
        if phase_lower == "change event application":
            return "CEA"
        elif phase_lower == "waiting to start change event application":
            return "waiting to<br>start CEA"
        elif phase_lower == "initializing collections and indexes":
            return "initializing collections<br>& indexes"
        
        # Split long phase names into 2 lines
        phase_capitalized = phase_name.capitalize()
        words = phase_capitalized.split()
        
        if len(words) <= 2:
            return phase_capitalized
        
        # Split roughly in the middle
        mid = len(words) // 2
        line1 = " ".join(words[:mid])
        line2 = " ".join(words[mid:])
        return f"{line1}<br>{line2}"
    
    vPhase = []
    vTs = []
    if len(phaseTransitions) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=8, color="black")), row=2, col=1)
        fig.update_layout(xaxis3=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis3=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:        
        for pt in phaseTransitions:
            phase_name = pt.get("phase", "")
            formatted_phase = format_phase_label(phase_name)
            vPhase.append(formatted_phase)
            ts = pt.get("ts")
            if isinstance(ts, Timestamp):
                vTs.append(datetime.fromtimestamp(ts.time, tz=timezone.utc))
            elif isinstance(ts, datetime):
                vTs.append(ts)
            else:
                vTs.append(None)
        fig.add_trace(go.Scatter(
            x=vPhase, 
            y=vTs, 
            mode='markers+text', 
            marker=dict(color='green', size=6),
            textfont=dict(size=8),
            textposition='top center'
        ), row=2, col=1)
    
    fig.update_xaxes(tickfont=dict(size=8), row=2, col=1)
    fig.update_yaxes(tickfont=dict(size=8), row=2, col=1)
    
    # Row 2, Col 2 is now empty (Verification moved to separate tab)
    
    # 4. Collections Progress (Row 2, Col 2)
    vProject1 = {"$project": {  "namespace": {  "$concat": ["$namespace.db", ".", "$namespace.coll"]  },  "partitionPhase": 1  }}
    vGroup1 = {"$group": {  "_id": "$namespace",  "phases": { "$addToSet": "$partitionPhase" }  } }
    vProject2 = {"$project": {  "_id": 0,  "namespace": "$_id",  "phases": {  "$arrayToObject": {  "$map": {  "input": "$phases",  "as": "phase",  "in": { "k": "$$phase", "v": 1 }  }  }  }  }}
    vProject3 = {"$project": {  "_id": 0,"namespace": 1,  "phases": {  "$mergeObjects": [  { "not started": 0, "in progress": 0, "done": 0 },  "$phases"  ]  }}  }

    vCollectionData = internalDbDst.partitions.aggregate([vProject1, vGroup1, vProject2, vProject3])
    vCollectionData = list(vCollectionData)

    vTypeProc = []
    vTypeValue = []
    if len(vCollectionData) == 0:
        fig.add_trace(go.Scatter(x=[0], y=[0], text="NO DATA", mode='text', textfont=dict(size=8, color="black")), row=2, col=2)
        fig.update_layout(xaxis4=dict(showgrid=False, zeroline=False, showticklabels=False), 
                          yaxis4=dict(showgrid=False, zeroline=False, showticklabels=False))
    else:
        NotStarted = 0
        InProgress = 0
        Done = 0
        for collec in vCollectionData:
            if ((collec["phases"]["in progress"] == 1) or (collec["phases"]["not started"] == 1 and collec["phases"]["done"] == 1)):
                InProgress += 1
            elif (collec["phases"]["not started"] == 1 and collec["phases"]["done"] != 1):
                NotStarted += 1
            else:
                Done += 1
            
        vTypeProc.append("Not Started")
        vTypeValue.append(NotStarted)
        vTypeProc.append("In Progress")
        vTypeValue.append(InProgress)
        vTypeProc.append("Completed")
        vTypeValue.append(Done)
        yMax = max(vTypeValue)

        fig.add_trace(go.Bar(
            x=vTypeProc, 
            y=vTypeValue,
            width=0.5,
            marker=dict(color=vTypeValue, colorscale='Oryel'),
            name='Collection Status',
            showlegend=False,
            text=vTypeValue,
            textposition='outside',
            textfont=dict(size=11)
        ), row=2, col=2)
        fig.update_xaxes(title_text="Process", title_font=dict(size=10), tickfont=dict(size=8), row=2, col=2)
        fig.update_yaxes(title_text="Totals", title_font=dict(size=10), tickfont=dict(size=8), row=2, col=2)
        fig.update_layout(yaxis4=dict(range=[0, yMax * 1.1]))
    
    # 5. Collection Copy Progress (Row 1, Col 2) - column chart
    if endpoint_url:
        try:
            # Reuse the endpoint_data from earlier if available, or fetch again
            if 'endpoint_data' not in locals():
                import requests
                url = f"http://{endpoint_url}"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                endpoint_data = response.json()
                progress = endpoint_data.get("progress", {})
            
            # Collection Copy chart
            collectionCopy = progress.get("collectionCopy", {})
            if collectionCopy and isinstance(collectionCopy, dict):
                estimatedTotalBytes = collectionCopy.get("estimatedTotalBytes", 0) or 0
                estimatedCopiedBytes = collectionCopy.get("estimatedCopiedBytes", 0) or 0
                remainingBytes = max(0, estimatedTotalBytes - estimatedCopiedBytes)
                
                if estimatedTotalBytes > 0:
                    totalValue, totalUnit = format_byte_size(estimatedTotalBytes)
                    copiedValue = convert_bytes(estimatedCopiedBytes, totalUnit)
                    remainingValue = convert_bytes(remainingBytes, totalUnit)

                    fig.add_trace(go.Bar(
                        x=["Copied", "Remaining"],
                        y=[copiedValue, remainingValue],
                        marker=dict(color=["#00A35C", "#C1C7C6"]),
                        showlegend=False,
                        text=[f"{copiedValue:.2f} {totalUnit}", f"{remainingValue:.2f} {totalUnit}"],
                        textposition='outside',
                        textfont=dict(size=9),
                        hovertemplate="%{x}: %{y:.2f} " + str(totalUnit) + "<extra></extra>"
                    ), row=1, col=2)
                    fig.update_xaxes(tickfont=dict(size=9), row=1, col=2)
                    fig.update_yaxes(title_text=f"Data ({totalUnit})", title_font=dict(size=9), tickfont=dict(size=8), row=1, col=2)
                else:
                    fig.add_trace(go.Scatter(
                        x=[0],
                        y=[0],
                        text="NO DATA",
                        mode='text',
                        textfont=dict(size=11, color="black"),
                        showlegend=False
                    ), row=1, col=2)
                    fig.update_layout(
                        xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False)
                    )
            else:
                fig.add_trace(go.Scatter(
                    x=[0],
                    y=[0],
                    text="NO DATA",
                    mode='text',
                    textfont=dict(size=11, color="black"),
                    showlegend=False
                ), row=1, col=2)
                fig.update_layout(
                    xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False)
                )
                
        except Exception as e:
            logger.warning(f"Could not fetch endpoint data for collection copy chart: {e}")
            fig.add_trace(go.Scatter(
                x=[0],
                y=[0],
                text="NO ENDPOINT DATA",
                mode='text',
                textfont=dict(size=11, color="black"),
                showlegend=False
            ), row=1, col=2)
            fig.update_layout(
                xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False)
            )
    else:
        fig.add_trace(go.Scatter(
            x=[0],
            y=[0],
            text="NO ENDPOINT CONFIGURED",
            mode='text',
            textfont=dict(size=11, color="black"),
            showlegend=False
        ), row=1, col=2)
        fig.update_layout(
            xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    
    # Update layout
    fig.update_layout(
        height=720,
        width=900,
        autosize=True,
        showlegend=False,
        plot_bgcolor="white",
        margin=dict(l=40, r=40, t=80, b=40)
    )
    fig.update_annotations(font=dict(size=11))
    # Position top-row titles above the charts
    for i in range(2):
        fig.layout.annotations[i].update(
            y=1.0, yanchor='bottom', yref='paper',
            yshift=10
        )

    # Convert the figure to JSON and return as Flask response
    plot_json = json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))
    return jsonify(plot_json)


def gatherEndpointMetrics(endpoint_url):
    """Fetch and display data from the Mongosync Progress Endpoint URL with Verification pie chart."""
    logger = logging.getLogger(__name__)
    
    # Create a figure for displaying endpoint data - Verification pie chart and table
    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[0.5, 0.5],
        specs=[[{"type": "pie"}],
               [{"type": "table"}]],
        subplot_titles=("Verification", "Verification Details"),
        horizontal_spacing=0.08,
        vertical_spacing=0.12
    )
    
    try:
        # Make HTTP GET request to the endpoint
        url = f"http://{endpoint_url}"
        logger.info(f"Fetching data from endpoint: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Debug logging to diagnose endpoint response
        logger.info(f"Endpoint response status: {response.status_code}")
        logger.info(f"Endpoint response keys: {list(data.keys())}")
        
        # Extract progress data - state/canCommit/canWrite are nested inside "progress"
        progress = data.get("progress", {})
        logger.info(f"Progress object keys: {list(progress.keys()) if progress else 'None'}")
        
        # Extract state information from progress object
        state = progress.get("state", "NO DATA")
        canCommit = progress.get("canCommit", False)
        canWrite = progress.get("canWrite", False)
        lag_time_seconds = progress.get("lagTimeSeconds")
        verification = progress.get("verification", {})
        verif_source = verification.get("source", {}) if verification else {}
        verif_dest = verification.get("destination", {}) if verification else {}
        phase = (progress.get("phase") or verif_dest.get("phase") or verif_source.get("phase") or "")
        if isinstance(phase, str):
            phase = phase.strip()
        else:
            phase = str(phase) if phase else ""

        logger.info(
            "Final state for traces: state=%s, canCommit=%s, canWrite=%s, phase=%s, lagTimeSeconds=%s",
            state,
            canCommit,
            canWrite,
            phase,
            lag_time_seconds,
        )
        
        # Add invisible text traces for JavaScript to parse state information.
        # Home parses by label text (not fixed indices), so order is not strict.
        fig.add_trace(go.Scatter(
            x=[0], y=[0],
            mode='text',
            text=[f"<b>Mongosync State:</b> {state}"],
            showlegend=False,
            hoverinfo='skip',
            textfont=dict(size=1, color='rgba(0,0,0,0)'),  # Invisible
            xaxis='x2',
            yaxis='y2'
        ))
        
        fig.add_trace(go.Scatter(
            x=[0], y=[0],
            mode='text',
            text=[f"<b>Can Write:</b> {str(canWrite).lower()}"],
            showlegend=False,
            hoverinfo='skip',
            textfont=dict(size=1, color='rgba(0,0,0,0)'),  # Invisible
            xaxis='x2',
            yaxis='y2'
        ))
        
        fig.add_trace(go.Scatter(
            x=[0], y=[0],
            mode='text',
            text=[f"<b>Can Commit:</b> {str(canCommit).lower()}"],
            showlegend=False,
            hoverinfo='skip',
            textfont=dict(size=1, color='rgba(0,0,0,0)'),  # Invisible
            xaxis='x2',
            yaxis='y2'
        ))

        fig.add_trace(go.Scatter(
            x=[0], y=[0],
            mode='text',
            text=[f"<b>Phase:</b> {phase}"],
            showlegend=False,
            hoverinfo='skip',
            textfont=dict(size=1, color='rgba(0,0,0,0)'),  # Invisible
            xaxis='x2',
            yaxis='y2'
        ))

        fig.add_trace(go.Scatter(
            x=[0], y=[0],
            mode='text',
            text=[f"<b>Lag Time Seconds:</b> {lag_time_seconds}"],
            showlegend=False,
            hoverinfo='skip',
            textfont=dict(size=1, color='rgba(0,0,0,0)'),  # Invisible
            xaxis='x2',
            yaxis='y2'
        ))

        # Create a hidden axis for the invisible traces
        fig.update_layout(
            xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
            yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False)
        )
        
        # Helper function to format values for display
        def format_value(value):
            if value is None:
                return "No Data"
            elif isinstance(value, bool):
                return str(value).capitalize()
            elif isinstance(value, dict):
                json_str = json.dumps(value, indent=2)
                return (json_str[:100] + "...").capitalize() if len(json_str) > 100 else json_str.capitalize()
            else:
                str_value = str(value).strip()
                if str_value == "" or str_value.upper() in ("N/A", "NULL", "NONE"):
                    return "No Data"
                return str_value.capitalize()
        
        # Helper function to get color based on value
        def get_color(key, value):
            if key == "state":
                if value == "RUNNING":
                    return "blue"
                elif value == "IDLE":
                    return "orange"
                elif value == "COMMITTED":
                    return "green"
                elif value == "PAUSED":
                    return "red"
            elif key in ["canCommit", "canWrite"]:
                return "green" if value else "red"
            return "black"
        
        # Helper function to format lag time in seconds to human-readable format
        def format_lag_time(seconds):
            if seconds is None:
                return "No Data"
            try:
                total_seconds = int(seconds)
                if total_seconds < 0:
                    return "0s"
                days, remainder = divmod(total_seconds, 86400)
                hours, remainder = divmod(remainder, 3600)
                minutes, secs = divmod(remainder, 60)
                parts = []
                if days > 0:
                    parts.append(f"{days}d")
                if hours > 0:
                    parts.append(f"{hours}h")
                if minutes > 0:
                    parts.append(f"{minutes}m")
                parts.append(f"{secs}s")
                return " ".join(parts)
            except (ValueError, TypeError):
                return "No Data"
        
        # Helper to format a dict as table columns (keys and values)
        def dict_to_table(data):
            if not data or not isinstance(data, dict):
                return ["No Data"], [""]
            keys = []
            values = []
            for k, v in data.items():
                keys.append(str(k))
                val_str = str(v) if v is not None else "No Data"
                values.append(val_str)
            return keys, values
        
        
        # Verification comparison table (source vs destination)
        verification = progress.get("verification", {})
        verif_source = verification.get("source", {}) if verification else {}
        verif_dest = verification.get("destination", {}) if verification else {}
        
        # Add Verification Pie Chart (Row 1)
        src_estimated_docs = verif_source.get("estimatedDocumentCount", 0) or 0 if verif_source else 0
        dst_estimated_docs = verif_dest.get("estimatedDocumentCount", 0) or 0 if verif_dest else 0
        verified_docs = dst_estimated_docs
        remaining_docs = max(0, src_estimated_docs - dst_estimated_docs)
        
        if verified_docs > 0 or remaining_docs > 0:
            fig.add_trace(go.Pie(
                labels=[f"Verified ({verified_docs:,})", f"Remaining ({remaining_docs:,})"],
                values=[verified_docs, remaining_docs],
                marker=dict(colors=["green", "lightgray"]),
                textinfo="label",
                texttemplate="%{label}",
                hovertemplate="%{label}<extra></extra>",
                textposition="inside",
                textfont=dict(size=11),
                hole=0.3,
                showlegend=False
            ), row=1, col=1)
        else:
            fig.add_trace(go.Pie(
                labels=["No Data"],
                values=[1],
                marker=dict(colors=["lightgray"]),
                textinfo="label",
                textfont=dict(size=12),
                showlegend=False
            ), row=1, col=1)
        
        # Define the fields to compare
        verif_fields = [
            ("phase", "Phase"),
            ("lagTimeSeconds", "Lag Time Seconds"), 
            ("totalCollectionCount", "Total Collection Count"),
            ("scannedCollectionCount", "Scanned Collection Count"),
            ("hashedDocumentCount", "Hashed Document Count"),
            ("estimatedDocumentCount", "Estimated Document Count")
        ]
        
        # Build table columns
        field_names = []
        source_values = []
        dest_values = []
        
        for field_key, field_label in verif_fields:
            field_names.append(field_label)
            
            # Get source value
            src_val = verif_source.get(field_key) if verif_source else None
            source_values.append(str(src_val) if src_val is not None else "No Data")
            
            # Get destination value
            dst_val = verif_dest.get(field_key) if verif_dest else None
            dest_values.append(str(dst_val) if dst_val is not None else "No Data")
        
        # Create verification comparison table (Row 2)
        if verification:
            fig.add_trace(go.Table(
                header=dict(values=["Field", "Source", "Destination"], font=dict(size=12, color='black')),
                cells=dict(values=[field_names, source_values, dest_values], align=['left'], font=dict(size=10, color='darkblue')),
                columnwidth=[1.5, 1, 1]
            ), row=2, col=1)
        else:
            fig.add_trace(go.Table(
                header=dict(values=["Field", "Source", "Destination"], font=dict(size=12, color='black')),
                cells=dict(values=[["Verification"], ["No Data"], ["No Data"]], align=['left'], font=dict(size=10, color='darkblue')),
                columnwidth=[1.5, 1, 1]
            ), row=2, col=1)
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout connecting to endpoint: {endpoint_url}")
        # Add No Data pie chart
        fig.add_trace(go.Pie(
            labels=["No Data"],
            values=[1],
            marker=dict(colors=["lightgray"]),
            textinfo="label",
            textfont=dict(size=12),
            showlegend=False
        ), row=1, col=1)
        fig.add_trace(go.Table(
            header=dict(values=["Error"], font=dict(size=12, color='black')),
            cells=dict(
                values=[['TIMEOUT - Could not reach endpoint']],
                align=['left'],
                font=dict(size=12, color='red'),
                height=40
            )
        ), row=2, col=1)
    except requests.exceptions.ConnectionError as e:
        error_msg = str(e).lower()
        
        # Check if this is a "connection refused" error (mongosync initializing)
        is_connection_refused = 'connection refused' in error_msg or 'couldn\'t connect' in error_msg or 'failed to connect' in error_msg
        
        if is_connection_refused:
            logger.warning(f"Mongosync API not yet available at {endpoint_url} (likely still initializing)")
            # Show initializing state instead of error
            fig.add_trace(go.Pie(
                labels=["Initializing"],
                values=[1],
                marker=dict(colors=["#FFC010"]),  # MongoDB yellow
                textinfo="label",
                textfont=dict(size=12),
                showlegend=False
            ), row=1, col=1)
            
            # Add hidden traces for state management (same as normal response)
            fig.add_trace(go.Scatter(
                x=[0], y=[0],
                mode='text',
                text=[f"<b>Mongosync State:</b> INITIALIZING"],
                showlegend=False,
                hoverinfo='skip',
                textfont=dict(size=1, color='rgba(0,0,0,0)'),
                xaxis='x2',
                yaxis='y2'
            ))
            fig.add_trace(go.Scatter(
                x=[0], y=[0],
                mode='text',
                text=[f"<b>Can Write:</b> false"],
                showlegend=False,
                hoverinfo='skip',
                textfont=dict(size=1, color='rgba(0,0,0,0)'),
                xaxis='x2',
                yaxis='y2'
            ))
            fig.add_trace(go.Scatter(
                x=[0], y=[0],
                mode='text',
                text=[f"<b>Can Commit:</b> false"],
                showlegend=False,
                hoverinfo='skip',
                textfont=dict(size=1, color='rgba(0,0,0,0)'),
                xaxis='x2',
                yaxis='y2'
            ))
            
            # Create hidden axis for the invisible traces
            fig.update_layout(
                xaxis2=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
                yaxis2=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False)
            )
            
            fig.add_trace(go.Table(
                header=dict(values=["Status"], font=dict(size=12, color='black')),
                cells=dict(
                    values=[['Mongosync is starting up. The API typically becomes available within 2-5 minutes. The dashboard will automatically connect once ready.']],
                    align=['left'],
                    font=dict(size=11, color='#FFC010'),  # MongoDB yellow
                    height=60
                )
            ), row=2, col=1)
        else:
            logger.error(f"Connection error to endpoint {endpoint_url}: {e}")
            # Add No Data pie chart for other connection errors
            fig.add_trace(go.Pie(
                labels=["No Data"],
                values=[1],
                marker=dict(colors=["lightgray"]),
                textinfo="label",
                textfont=dict(size=12),
                showlegend=False
            ), row=1, col=1)
            fig.add_trace(go.Table(
                header=dict(values=["Error"], font=dict(size=12, color='black')),
                cells=dict(
                    values=[[f'CONNECTION ERROR: {str(e)[:100]}']],
                    align=['left'],
                    font=dict(size=12, color='red'),
                    height=40
                )
            ), row=2, col=1)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error to endpoint {endpoint_url}: {e}")
        # Add No Data pie chart
        fig.add_trace(go.Pie(
            labels=["No Data"],
            values=[1],
            marker=dict(colors=["lightgray"]),
            textinfo="label",
            textfont=dict(size=12),
            showlegend=False
        ), row=1, col=1)
        fig.add_trace(go.Table(
            header=dict(values=["Error"], font=dict(size=12, color='black')),
            cells=dict(
                values=[['REQUEST ERROR']],
                align=['left'],
                font=dict(size=12, color='red'),
                height=40
            )
        ), row=2, col=1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response from endpoint {endpoint_url}: {e}")
        # Add No Data pie chart
        fig.add_trace(go.Pie(
            labels=["No Data"],
            values=[1],
            marker=dict(colors=["lightgray"]),
            textinfo="label",
            textfont=dict(size=12),
            showlegend=False
        ), row=1, col=1)
        fig.add_trace(go.Table(
            header=dict(values=["Error"], font=dict(size=12, color='black')),
            cells=dict(
                values=[['INVALID JSON RESPONSE']],
                align=['left'],
                font=dict(size=12, color='red'),
                height=40
            )
        ), row=2, col=1)
    except Exception as e:
        logger.error(f"Unexpected error fetching endpoint data: {e}")
        # Add No Data pie chart
        fig.add_trace(go.Pie(
            labels=["No Data"],
            values=[1],
            marker=dict(colors=["lightgray"]),
            textinfo="label",
            textfont=dict(size=12),
            showlegend=False
        ), row=1, col=1)
        fig.add_trace(go.Table(
            header=dict(values=["Error"], font=dict(size=12, color='black')),
            cells=dict(
                values=[[str(e)[:50]]],
                align=['left'],
                font=dict(size=12, color='red'),
                height=40
            )
        ), row=2, col=1)
    
    # Update subplot title annotations
    fig.update_annotations(font=dict(size=12))
    
    # Update layout - compact size for maximized window without scrolling
    fig.update_layout(
        height=600,
        width=900,
        autosize=True,
        showlegend=False,
        plot_bgcolor="white",
        margin=dict(l=40, r=40, t=100, b=40)
    )
    
    # Convert the figure to JSON and return as Flask response
    plot_json = json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))
    return jsonify(plot_json)


def plotMetrics(has_connection_string=True, has_endpoint_url=False):
    """
    Render the metrics page with tab configuration.
    
    Credentials are stored in server-side session for security - 
    they are never passed to the client-side JavaScript.
    """
    # Use the centralized configuration
    from app_config import REFRESH_TIME

    refreshTime = REFRESH_TIME
    refreshTimeMs = str(int(refreshTime) * 1000)
    
    return render_template('metrics.html', 
                         refresh_time=refreshTime, 
                         refresh_time_ms=refreshTimeMs,
                         has_connection_string=has_connection_string,
                         has_endpoint_url=has_endpoint_url)