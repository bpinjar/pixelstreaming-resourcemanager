import logging
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import ORJSONResponse
from dotenv import load_dotenv
from typing import Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Load variables from the .env file
load_dotenv()

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
        self.active_streams = {
            'Infosys': {},  # Maps stream ID to requester
            'Dubaimall': {}  # Maps stream ID to requester
        }
        self.available_streams = {
            'Infosys': {},  # Maps stream ID to requester
            'Dubaimall': {}  # Maps stream ID to requester
        }
        self.requester_streams = {}  # Maps requester_id to (stream_id, service)

    def check_available_stream(self, service: str) -> Tuple[Optional[str], Optional[str]]:
        logging.info(f"Checking available streams for service: {service}")
        if self.available_streams[service]:
            stream_id, ip_address = next(iter(self.available_streams[service].items()))
            logging.info(f"Found available stream: {stream_id} with IP: {ip_address}")
            return stream_id, ip_address
        else:
            logging.warning(f"No available streams found for service: {service}")
            return None, None

    def request_stream(self, requester_id: str, service: str) -> Optional[Dict[str, str]]:
        logging.info(f"Requester {requester_id} requesting stream for service: {service}")
        
        if requester_id in self.requester_streams:
            stream_id, existing_service = self.requester_streams[requester_id]
            if existing_service == service:
                logging.info(f"Requester {requester_id} already has an active stream: {stream_id} for service: {service}")
                return {"ip": self.active_streams[service][stream_id], "stream_id": stream_id}
            else:
                error_message = f"Requester already has an active stream with {existing_service}. Please release it before requesting a new one."
                logging.error(error_message)
                raise HTTPException(status_code=400, detail=error_message)

        stream_id, ip = self.check_available_stream(service)
        if stream_id:
            self.available_streams[service].pop(stream_id)
            self.active_streams[service][stream_id] = ip
            self.requester_streams[requester_id] = (stream_id, service)
            logging.info(f"Allocated stream {stream_id} with IP {ip} to requester {requester_id}")
            return {"ip": ip, "stream_id": stream_id}

        logging.warning(f"No streams available to allocate for service: {service}")
        return None

    def release_stream(self, requester_id: str) -> bool:
        logging.info(f"Requester {requester_id} is releasing their stream")
        if requester_id in self.requester_streams:
            stream_id, service = self.requester_streams.pop(requester_id)
            ip_address = self.active_streams[service].pop(stream_id)
            self.available_streams[service][stream_id] = ip_address
            logging.info(f"Stream {stream_id} released and made available again")
            return True
        logging.warning(f"Requester {requester_id} has no active streams to release")
        return False

    def get_active_streams(self) -> Dict[str, Dict[str, str]]:
        logging.info("Fetching all active streams")
        return self.active_streams

    def get_requester_streams(self) -> Dict[str, Tuple[str, str]]:
        logging.info("Fetching all requester streams")
        return self.requester_streams

# Initialize the PixelStreamManager
manager = PixelStreamResourceManager()

# Add some mock available streams for testing
manager.available_streams['Infosys']['stream1'] = 'https://share.streampixel.io/66f57133ee04dcc4dd642b48'
manager.available_streams['Infosys']['stream2'] = 'https://share.streampixel.io/66f57133ee04dcc4dd642b49'
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
async def rm_release_allocated_stream(requester_id: str = Form(...)):
    """Release a previously allocated stream for a requester."""
    if requester_id in manager.requester_streams:
        stream_id, service = manager.requester_streams.pop(requester_id)
        ip_address = manager.active_streams[service].pop(stream_id)
        manager.available_streams[service][stream_id] = ip_address
        
        logging.info(
            f"Stream {stream_id} with IP {ip_address} released successfully and is now available for service: {service}"
        )
        
        return {
            "message": f"Stream released and available again",
            "requester_id": requester_id,
            "stream_id": stream_id,
            "ip": ip_address
        }
    else:
        logging.error(
            f"Requester {requester_id} does not have any active streams to release."
        )
        raise HTTPException(status_code=404, detail=f"Requester {requester_id} does not have any active streams.")

@app.get("/rm_list_active_streams", response_class=ORJSONResponse)
def rm_list_active_streams():
    """Retrieve all active streams for all services."""
    active_streams = manager.get_active_streams()
    logging.info("Returning active streams")
    return active_streams

@app.get("/rm_list_requester_streams", response_class=ORJSONResponse)
def rm_list_requester_streams():
    """Retrieve all streams currently allocated to requesters."""
    requester_streams = manager.get_requester_streams()
    logging.info("Returning requester streams")
    result = {
        requester_id: {
            "stream_id": stream_id,
            "service": service,
            "ip": manager.active_streams[service][stream_id]
        }
        for requester_id, (stream_id, service) in requester_streams.items()
    }
    return result


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8081)
