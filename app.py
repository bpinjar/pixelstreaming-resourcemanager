from fastapi import FastAPI, HTTPException, Path, Body, Query
from pydantic import BaseModel, Field
from typing import Optional, List
import pymongo
from bson.objectid import ObjectId
from datetime import datetime
import uuid  # Import the UUID library for unique IDs

import uvicorn
import logging


from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()
import os
# Retrieve MongoDB URI from environment variables

# MongoDB Configuration


uri = os.getenv("MONGODB_URI")
client = pymongo.MongoClient(uri)
db = client['pixel_stream_manager']
streams_collection = db['streams']
cost_collection = db['costs']

# FastAPI Initialization
app = FastAPI(title="Pixel Streaming Manager", description="API for managing pixel streaming services.", version="1.0")

# Pydantic Models
class StreamBase(BaseModel):
    streamName: str
    ip: str
    pool: str
    lastUsedTime: Optional[str] = ""
    requester: Optional[str] = None

class StreamUpdate(BaseModel):
    streamName: Optional[str]
    ip: Optional[str]
    pool: Optional[str]
    lastUsedTime: Optional[str]
    requester: Optional[str]

class StreamResponse(StreamBase):
    id: str = Field(..., alias="_id")

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper Functions
def format_stream(stream):
    stream["_id"] = str(stream["_id"])
    return stream

@app.get("/getPool", response_model=dict)
def get_pool():
    """
    Fetch unique pool names along with the number of entries for each pool.
    Example response:
    {
        "production": 5,
        "development": 4
    }
    """
    try:
        # Aggregate the pool names and count the number of entries for each pool
        pipeline = [
            {"$group": {"_id": "$pool", "count": {"$sum": 1}}}  # Group by pool and count entries
        ]
        
        result = list(streams_collection.aggregate(pipeline))
        
        # Convert the result into a dictionary with pool names as keys and counts as values
        pool_counts = {entry["_id"]: entry["count"] for entry in result}
        
        logger.info(f"Pool counts retrieved: {pool_counts}")
        return pool_counts
    
    except Exception as e:
        logger.error(f"Error retrieving pool data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving pool data: {str(e)}")


@app.delete("/deletePool/{pool_name}", response_model=dict)
def delete_pool(pool_name: str = Path(..., description="The name of the pool to delete services for")):
    """
    Delete all services that match the given pool name.
    """
    result = streams_collection.delete_many({"pool": pool_name})
    
    if result.deleted_count == 0:
        logger.warning(f"No services found for the pool: {pool_name}")
        raise HTTPException(status_code=404, detail="No services found for the specified pool")
    
    logger.info(f"All services in pool '{pool_name}' deleted")
    return {"message": f"All services in pool '{pool_name}' deleted"}


@app.get("/getActiveStreams", response_model=List[StreamResponse])
def get_active_streams():
    streams = list(streams_collection.find({"requester": {"$ne": None}}))
    logger.info(f"Retrieved {len(streams)} active streams")
    return [format_stream(stream) for stream in streams]

@app.get("/getAvailableStreams", response_model=List[StreamResponse])
def get_available_streams():
    streams = list(streams_collection.find({"requester": None}))
    logger.info(f"Retrieved {len(streams)} available streams")
    return [format_stream(stream) for stream in streams]

@app.post("/addStream", response_model=StreamResponse)
def add_stream(stream: StreamBase):
    """
    Add a new stream if the streamName is unique.
    Convert an empty requester field to None before saving.
    """
    # Convert empty requester string to None
    stream_data = stream.dict()
    if stream_data.get("requester") == "":
        stream_data["requester"] = None
    
    # Check if a stream with the same streamName already exists
    existing_stream = streams_collection.find_one({"streamName": stream_data["streamName"]})
    if existing_stream:
        logger.warning(f"Stream with name {stream_data['streamName']} already exists")
        raise HTTPException(status_code=400, detail="Stream with this streamName already exists")
    
    # Insert the new stream into the database
    result = streams_collection.insert_one(stream_data)
    
    # Fetch the newly inserted stream
    new_stream = streams_collection.find_one({"_id": result.inserted_id})
    
    logger.info(f"Stream {new_stream['streamName']} added successfully")
    return format_stream(new_stream)


@app.put("/updateStream", response_model=dict)
def update_stream(stream_name: str = Query(..., description="The name of the stream to update"), 
                  pool_name: str = Query(..., description="The pool name of the stream to update"), 
                  updates: StreamUpdate = Body(...)):
    """
    Update a stream using streamName and pool.
    Only updates fields that are provided in the request body.
    """
    result = streams_collection.update_one(
        {"streamName": stream_name, "pool": pool_name},
        {"$set": updates.dict(exclude_unset=True)}
    )
    
    if result.matched_count == 0:
        logger.warning(f"Stream {stream_name} in pool {pool_name} not found for update")
        raise HTTPException(status_code=404, detail="Stream not found")
    
    logger.info(f"Stream {stream_name} in pool {pool_name} updated")
    return {"message": "Stream updated"}

@app.put("/releaseStream", response_model=dict)
def release_stream(
    pool: str = Query(..., description="The pool name of the stream to release"),
    stream_name: str = Query(..., description="The name of the stream to release")
):
    """
    Release a stream by setting its requester to None based on pool and streamName.
    """
    result = streams_collection.update_one(
        {"pool": pool, "streamName": stream_name},
        {"$set": {"requester": None}}
    )

    if result.matched_count == 0:
        logger.warning(f"Stream {stream_name} in pool {pool} not found for release")
        raise HTTPException(status_code=404, detail="Stream not found")

    logger.info(f"Stream {stream_name} in pool {pool} released")
    return {"message": "Stream released"}


@app.get("/getStreamDetails", response_model=StreamResponse)
def get_stream_details(
    stream_name: str = Query(..., description="The name of the stream to fetch details for"),
    pool: str = Query(..., description="The pool name of the stream to fetch details for")
):
    """
    Fetch stream details based on streamName and pool.
    """
    stream = streams_collection.find_one({"streamName": stream_name, "pool": pool})
    if not stream:
        logger.warning(f"Stream {stream_name} in pool {pool} not found")
        raise HTTPException(status_code=404, detail="Stream not found")
    
    logger.info(f"Stream details for {stream_name} in pool {pool} retrieved")
    return format_stream(stream)


@app.get("/getRequestorDetails/{requester}", response_model=List[StreamResponse])
def get_requestor_details(requester: str):
    streams = list(streams_collection.find({"requester": requester}))
    logger.info(f"Retrieved {len(streams)} streams for requester {requester}")
    return [format_stream(stream) for stream in streams]

@app.put("/updateLastUsed", response_model=dict)
def update_last_used(
    pool: str = Query(..., description="The pool name of the stream to update"),
    stream_name: str = Query(..., description="The name of the stream to update")
):
    """
    Update the last used time for a stream based on its pool and streamName.
    """
    current_time = datetime.utcnow().isoformat()

    result = streams_collection.update_one(
        {"pool": pool, "streamName": stream_name},
        {"$set": {"lastUsedTime": current_time}}
    )

    if result.matched_count == 0:
        logger.warning(f"Stream {stream_name} in pool {pool} not found for last used time update")
        raise HTTPException(status_code=404, detail="Stream not found")

    logger.info(f"Last used time for stream {stream_name} in pool {pool} updated to {current_time}")
    return {"message": "Last used time updated", "lastUsedTime": current_time}

@app.post("/requestStream", response_model=dict)
def request_stream(requester: str = Body(..., embed=True)):
    """
    Allocate a stream to the requester if available.
    If the requester already has an assigned stream, return the stream details.
    """
    existing_stream = streams_collection.find_one({"requester": requester})
    if existing_stream:
        logger.info(f"Requester {requester} already has an assigned stream")
        return {"streamType": "Stream already assigned", "streamDetails": format_stream(existing_stream)}

    available_stream = streams_collection.find_one({"requester": None})
    if not available_stream:
        logger.error("No available streams")
        raise HTTPException(status_code=404, detail="No available streams")

    # Assign the stream to the requester
    streams_collection.update_one({"_id": available_stream["_id"]}, {"$set": {"requester": requester}})
    
    
    # Log to cost_collection
    cost_collection.insert_one({
        "id": str(uuid.uuid4()),  # Generate a unique ID
        "requester": requester,
        "streamName": available_stream["streamName"],
        "pool": available_stream["pool"],
        "timestamp": datetime.utcnow().isoformat(),
    })
    
    
    logger.info(f"Stream {available_stream['streamName']} assigned to requester {requester}")
    return {"message": "Stream assigned", "streamDetails": format_stream(available_stream)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
