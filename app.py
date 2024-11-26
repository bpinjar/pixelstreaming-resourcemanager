import logging
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import ORJSONResponse
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import ORJSONResponse
import pymongo
from typing import Dict, Optional, Tuple
from datetime import datetime, timezone
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Load variables from the .env file
load_dotenv()

# MongoDB Configuration
uri = 'mongodb+srv://aetiustestadmin:tWFn4YzTyYw8UDhg@aetiustestdb.xi8oe.mongodb.net/?retryWrites=true&w=majority&loadbalanced=true'
#client = MongoClient("mongodb://localhost:27017")  # Replace with your MongoDB connection string
client = pymongo.MongoClient(uri)
db = client['pixel_stream_manager']
streams_collection = db['streams']

# Initialize FastAPI app
app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##################################
###### Resource Manager class
##################################

class PixelStreamResourceManager:
    def __init__(self):
        self.services = []  # Will be initialized from the database
        self.reload_services()

    def reload_services(self):
        """Reload the services list from the database."""
        self.services = [doc["service"] for doc in streams_collection.find({}, {"service": 1})]
        logging.info(f"Reloaded services from database: {self.services}")

    def initialize_collections(self):
        """Ensure MongoDB has required structure."""
        for service in self.services:
            if not streams_collection.find_one({"service": service}):
                streams_collection.insert_one({
                    "service": service,
                    "available_streams": {},  # Stream ID -> IP
                    "active_streams": {},     # Stream ID -> IP
                    "requester_streams": {}   # Requester ID -> (Stream ID, Service)
                })


    def get_service_data(self, service: str):
        """Retrieve service data from MongoDB."""
        return streams_collection.find_one({"service": service})

    def update_service_data(self, service: str, data: Dict):
        """Update service data in MongoDB."""
        streams_collection.update_one({"service": service}, {"$set": data})

    def check_available_stream(self, service: str) -> Tuple[Optional[str], Optional[str]]:
        logging.info(f"Checking available streams for service: {service}")
        service_data = self.get_service_data(service)
        available_streams = service_data.get("available_streams", {})
        if available_streams:
            stream_id, ip_address = next(iter(available_streams.items()))
            logging.info(f"Found available stream: {stream_id} with IP: {ip_address}")
            return stream_id, ip_address
        else:
            logging.warning(f"No available streams found for service: {service}")
            return None, None

    from datetime import datetime

    def request_stream(self, requester_id: str, service: str) -> Optional[Dict[str, str]]:
        logging.info(f"Requester {requester_id} requesting stream for service: {service}")
        service_data = self.get_service_data(service)
        requester_streams = service_data.get("requester_streams", {})
        
        if requester_id in requester_streams:
            stream_data = requester_streams[requester_id]
            stream_id = stream_data["stream_id"]
            existing_service = stream_data["service"]
            if existing_service == service:
                logging.info(f"Requester {requester_id} already has an active stream: {stream_id} for service: {service}")
                active_streams = service_data.get("active_streams", {})
                return {"ip": active_streams[stream_id], "stream_id": stream_id}
            else:
                error_message = f"Requester already has an active stream with {existing_service}. Please release it before requesting a new one."
                logging.error(error_message)
                raise HTTPException(status_code=400, detail=error_message)

        stream_id, ip = self.check_available_stream(service)
        if stream_id:
            # Update available_streams and active_streams in MongoDB
            available_streams = service_data.get("available_streams", {})
            active_streams = service_data.get("active_streams", {})
            requester_streams = service_data.get("requester_streams", {})

            available_streams.pop(stream_id)
            active_streams[stream_id] = ip
            requester_streams[requester_id] = {
                "stream_id": stream_id,
                "service": service,
                "last_used": datetime.now(timezone.utc).isoformat()  # Store current timestamp
            }

            self.update_service_data(service, {
                "available_streams": available_streams,
                "active_streams": active_streams,
                "requester_streams": requester_streams
            })
            logging.info(f"Allocated stream {stream_id} with IP {ip} to requester {requester_id}")
            return {"ip": ip, "stream_id": stream_id}
        else:
            logging.warning(f"No streams available to allocate for service: {service}")
            return None


    def release_stream_by_id(self, stream_id: str) -> bool:
        logging.info(f"Releasing stream with ID: {stream_id}")
        for service in self.services:
            service_data = self.get_service_data(service)
            active_streams = service_data.get("active_streams", {})
            available_streams = service_data.get("available_streams", {})
            requester_streams = service_data.get("requester_streams", {})
            
            if stream_id in active_streams:
                ip_address = active_streams.pop(stream_id)
                available_streams[stream_id] = ip_address
                
                # Remove from requester_streams
                requester_to_remove = None
                for requester_id, stream_data in requester_streams.items():
                    if stream_data["stream_id"] == stream_id:
                        requester_to_remove = requester_id
                        break
                if requester_to_remove:
                    requester_streams.pop(requester_to_remove)

                # Update the service data in the database
                self.update_service_data(service, {
                    "available_streams": available_streams,
                    "active_streams": active_streams,
                    "requester_streams": requester_streams
                })
                logging.info(f"Stream {stream_id} released and made available again")
                return True
        logging.warning(f"No active stream found with ID: {stream_id}")
        return False

    def get_active_streams(self) -> Dict[str, Dict[str, str]]:
        logging.info("Fetching all active streams")
        active_streams = {}
        for service in self.services:
            service_data = self.get_service_data(service)
            active_streams[service] = service_data.get("active_streams", {})
        return active_streams

    def get_available_streams(self) -> Dict[str, Dict[str, str]]:
        """Retrieve all available streams for all services."""
        logging.info("Fetching all available streams")
        available_streams = {}
        for service in self.services:
            service_data = self.get_service_data(service)
            available_streams[service] = service_data.get("available_streams", {})
        return available_streams
    
    def release_stream_by_service_and_id(self, service: str, stream_id: str) -> bool:
        logging.info(f"Releasing stream with ID: {stream_id} for service: {service}")
        
        # Retrieve the service data from MongoDB
        service_data = self.get_service_data(service)
        if not service_data:
            logging.warning(f"Service '{service}' not found in the database.")
            return False
        
        # Retrieve current active streams, available streams, and requester streams
        active_streams = service_data.get("active_streams", {})
        available_streams = service_data.get("available_streams", {})
        requester_streams = service_data.get("requester_streams", {})
        
        # Check if the stream is in active streams
        if stream_id in active_streams:
            # Remove the stream from active streams
            ip_address = active_streams.pop(stream_id)
            
            # Add the stream back to available streams
            available_streams[stream_id] = ip_address
            
            # Remove the stream from requester_streams if any requester is using it
            requester_to_remove = None
            for requester_id, stream_data in requester_streams.items():
                if stream_data["stream_id"] == stream_id:
                    requester_to_remove = requester_id
                    break
            
            # If the stream was found in requester_streams, remove it
            if requester_to_remove:
                requester_streams.pop(requester_to_remove)
            
            # Update the service data in MongoDB
            self.update_service_data(service, {
                "active_streams": active_streams,
                "available_streams": available_streams,
                "requester_streams": requester_streams
            })
            
            logging.info(f"Stream {stream_id} released and made available again for service '{service}'")
            return True
        
        else:
            logging.warning(f"Stream {stream_id} not found in active streams for service '{service}'")
            return False



# Initialize the PixelStreamManager
manager = PixelStreamResourceManager()
manager.initialize_collections()

# # Prepopulate some mock streams
# streams_collection.update_one({"service": "Infosys"}, {"$set": {
#     "available_streams": {
#         "stream1": "https://share.streampixel.io/66f57133ee04dcc4dd642b48",
#         "stream2": "https://share.streampixel.io/66f57133ee04dcc4dd642b49"
#     }
# }})




import json
@app.post("/rm_add_available_streams", response_class=ORJSONResponse)
async def add_available_streams(
    service: str = Form(...), 
    stream_id: str = Form(...), 
    stream_value: str = Form(...)
):
    """
    Add a new stream (with a specific stream_id and its value) to the available_streams for a specific service.
    Ensures that the stream_id is unique across both available and active streams.
    """
    try:
        # Retrieve the current document for the service
        service_document = streams_collection.find_one({"service": service})

        if not service_document:
            raise HTTPException(status_code=404, detail=f"Service '{service}' not found.")

        # Get the current available_streams and active_streams, or initialize empty dictionaries
        current_available_streams = service_document.get("available_streams", {})
        current_active_streams = service_document.get("active_streams", {})

        # Check if the stream_id already exists in available or active streams
        if stream_id in current_available_streams or stream_id in current_active_streams:
            raise HTTPException(
                status_code=400,
                detail=f"Stream ID '{stream_id}' is already in use (either in available or active streams)."
            )

        # Add the new stream to available_streams
        updated_streams = {**current_available_streams, stream_id: stream_value}

        # Update the database with the new stream
        streams_collection.update_one(
            {"service": service},
            {"$set": {"available_streams": updated_streams}}
        )

        return {
            "message": f"Stream '{stream_id}' successfully added for service '{service}'.",
            "updated_streams": updated_streams
        }

    except Exception as e:
        logging.error(f"Error in add_available_streams: {str(e)}")
        raise HTTPException(status_code=500, detail="Error in add_available_streams")
    
@app.post("/rm_update_available_streams", response_class=ORJSONResponse)
async def update_available_streams(
    service: str = Form(...), 
    stream_id: str = Form(...), 
    stream_value: str = Form(...)
):
    """
    Update an existing stream in the available_streams for a specific service.
    Ensures the stream_id exists before updating its value.
    """
    try:
        # Retrieve the current document for the service
        service_document = streams_collection.find_one({"service": service})

        if not service_document:
            raise HTTPException(status_code=404, detail=f"Service '{service}' not found.")

        # Get the current available_streams
        current_streams = service_document.get("available_streams", {})

        # Check if the stream_id exists in available_streams
        if stream_id not in current_streams:
            raise HTTPException(
                status_code=404,
                detail=f"Stream ID '{stream_id}' not found in available streams."
            )

        # Update the stream with the new value
        current_streams[stream_id] = stream_value

        # Save the updated streams back to the database
        streams_collection.update_one(
            {"service": service},
            {"$set": {"available_streams": current_streams}}
        )

        return {
            "message": f"Stream '{stream_id}' successfully updated for service '{service}'.",
            "updated_streams": current_streams
        }

    except Exception as e:
        logging.error(f"Error in update_available_streams: {str(e)}")
        raise HTTPException(status_code=500, detail="Error in update_available_streams")

    
@app.get("/rm_list_available_streams", response_class=ORJSONResponse)
def rm_list_available_streams():
    """Retrieve all available streams for all services."""
    available_streams = manager.get_available_streams()
    return available_streams

@app.post("/rm_update_last_used", response_class=ORJSONResponse)
async def rm_update_last_used(service: str = Form(...), stream_id: str = Form(...)):
    """
    Update the last_used timestamp of a given stream by its service and stream_id.
    """
    updated = False
    current_time = datetime.now(timezone.utc).isoformat()  # Current UTC time in ISO format

    # Retrieve the service data for the provided service name
    service_data = manager.get_service_data(service)
    
    if not service_data:
        logging.error(f"Service '{service}' not found")
        raise HTTPException(status_code=404, detail=f"Service '{service}' not found.")

    requester_streams = service_data.get("requester_streams", {})

    # Search for the stream_id within the specified service's requester streams
    for requester_id, stream_info in requester_streams.items():
        if stream_info["stream_id"] == stream_id:
            stream_info["last_used"] = current_time  # Update the timestamp
            requester_streams[requester_id] = stream_info

            # Update the database with the modified data
            manager.update_service_data(service, {"requester_streams": requester_streams})
            updated = True
            logging.info(f"Updated last_used for stream_id {stream_id} in service {service}")
            break

    if not updated:
        logging.error(f"Stream with ID {stream_id} not found in service {service}")
        raise HTTPException(status_code=404, detail="Stream not found.")

    return {"message": f"last_used updated for stream_id {stream_id} in service {service}", "timestamp": current_time}


@app.post("/rm_delete_stream", response_class=ORJSONResponse)
async def delete_stream(service: str = Form(...), stream_id: str = Form(...)):
    """
    Delete a specific stream from the available_streams for a specific service.
    Ensures the stream_id exists before attempting to delete it.
    """
    try:
        # Retrieve the current document for the service
        service_document = streams_collection.find_one({"service": service})

        if not service_document:
            raise HTTPException(status_code=404, detail=f"Service '{service}' not found.")

        # Get the current available_streams
        current_streams = service_document.get("available_streams", {})

        # Check if the stream_id exists in available_streams
        if stream_id not in current_streams:
            raise HTTPException(
                status_code=404,
                detail=f"Stream ID '{stream_id}' not found in available streams."
            )

        # Remove the stream from available_streams
        del current_streams[stream_id]

        # Save the updated streams back to the database
        streams_collection.update_one(
            {"service": service},
            {"$set": {"available_streams": current_streams}}
        )

        return {
            "message": f"Stream '{stream_id}' successfully deleted from service '{service}'.",
            "updated_streams": current_streams
        }

    except Exception as e:
        logging.error(f"Error in delete_stream: {str(e)}")
        raise HTTPException(status_code=500, detail="Error in delete_stream")

@app.post('/rm_allocate_stream', response_class=ORJSONResponse)
async def rm_allocate_stream(requester_id: str = Form(...), service: str = Form(...)):       
    """Allocate a stream to a requester for a specific service."""
    ip = manager.request_stream(requester_id, service)
    if ip:
        logging.info(f"Stream allocated for requester {requester_id}: {ip}")
        return {"stream_details": ip}
    else:
        logging.error(f"No available streams found for service: {service}")
        raise HTTPException(status_code=404, detail="No available streams found for the specified service.")

@app.post("/rm_release_allocated_stream", response_class=ORJSONResponse)
async def rm_release_allocated_stream(service: str = Form(...), stream_id: str = Form(...)):
    """Release a previously allocated stream by its service and stream ID."""
    # Assuming the manager.release_stream_by_service_and_id is a function that releases the stream based on service and stream_id.
    if manager.release_stream_by_service_and_id(service, stream_id):
        return {"message": f"Stream '{stream_id}' for service '{service}' released successfully."}
    else:
        raise HTTPException(status_code=404, detail=f"Stream '{stream_id}' for service '{service}' not found or already released.")

@app.get("/rm_list_active_streams", response_class=ORJSONResponse)
def rm_list_active_streams():
    """Retrieve all active streams for all services."""
    return manager.get_active_streams()

@app.post("/rm_add_service", response_class=ORJSONResponse)
async def add_service(service: str = Form(...)):
    """
    Add a new service to the system. Initializes empty collections for the new service.
    """
    try:
        # Check if the service already exists
        existing_service = streams_collection.find_one({"service": service})
        if existing_service:
            raise HTTPException(status_code=400, detail=f"Service '{service}' already exists.")

        # Initialize the new service with empty collections for available streams, active streams, and requester streams
        new_service_data = {
            "service": service,
            "available_streams": {},  # Empty initially
            "active_streams": {},     # Empty initially
            "requester_streams": {}   # Empty initially
        }

        # Insert the new service into the database
        streams_collection.insert_one(new_service_data)

        logging.info(f"Service '{service}' added successfully.")
        return {
            "message": f"Service '{service}' successfully added.",
            "service": service
        }

    except Exception as e:
        logging.error(f"Error in add_service: {str(e)}")
        raise HTTPException(status_code=500, detail="Error in add_service")


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8081)
