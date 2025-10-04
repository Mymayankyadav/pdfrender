from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import requests
from pdf2image import convert_from_bytes
import base64
from io import BytesIO
from typing import List, Optional
import re

# Make sure to import this middleware
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="PDF to Images Converter", version="1.0.0")

origins = [
    # You can be more specific here for production,
    # but "*" is fine for development.
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)


class PDFRequest(BaseModel):
    page_range: str  # Format: "1-3,5,7-9"
    url: HttpUrl
    dpi: int = 200

class PDFResponse(BaseModel):
    images: List[str]  # List of base64 encoded images
    total_pages_processed: int
    pages: List[int]   # Actual page numbers processed

def parse_page_range(page_range: str, max_pages: int) -> List[int]:
    """
    Parse page range string like "1-3,5,7-9" into list of page numbers
    Pages are 1-indexed in input, but we convert to 0-indexed for internal use
    """
    pages = set()
    
    # Remove spaces and split by comma
    parts = page_range.replace(' ', '').split(',')
    
    for part in parts:
        if '-' in part:
            # Range like "1-3"
            start_end = part.split('-')
            if len(start_end) != 2:
                raise ValueError(f"Invalid range: {part}")
            
            try:
                start = int(start_end[0])
                end = int(start_end[1])
            except ValueError:
                raise ValueError(f"Invalid numbers in range: {part}")
            
            if start < 1 or end > max_pages or start > end:
                raise ValueError(f"Range {part} out of valid bounds (1-{max_pages})")
            
            pages.update(range(start, end + 1))
        else:
            # Single page
            try:
                page = int(part)
            except ValueError:
                raise ValueError(f"Invalid page number: {part}")
            
            if page < 1 or page > max_pages:
                raise ValueError(f"Page {page} out of valid bounds (1-{max_pages})")
            
            pages.add(page)
    
    return sorted(pages)

@app.post("/convert-pdf", response_model=PDFResponse)
async def convert_pdf_to_images(request: PDFRequest):
    try:
        # Download PDF
        response = requests.get(str(request.url))
        response.raise_for_status()
        
        pdf_bytes = response.content
        
        # First, convert all pages to get total page count
        all_pages = convert_from_bytes(pdf_bytes, dpi=request.dpi)
        total_pages = len(all_pages)
        
        # Parse page range
        requested_pages = parse_page_range(request.page_range, total_pages)
        
        if not requested_pages:
            raise HTTPException(status_code=400, detail="No valid pages in the specified range")
        
        # Convert only the requested pages
        # Note: pdf2page uses 0-indexed pages, but our input is 1-indexed
        pages_to_convert = [page - 1 for page in requested_pages]
        images = convert_from_bytes(
            pdf_bytes, 
            dpi=request.dpi, 
            first_page=min(pages_to_convert) + 1,  # pdf2image uses 1-indexed for first_page/last_page
            last_page=max(pages_to_convert) + 1,
            fmt='PNG'
        )
        
        # Convert images to base64
        base64_images = []
        for img in images:
            buffered = BytesIO()
            img.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            base64_images.append(img_base64)
        
        return PDFResponse(
            images=base64_images,
            total_pages_processed=len(base64_images),
            pages=requested_pages
        )
        
    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to download PDF: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/")
async def root():
    return {"message": "PDF to Images Converter API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
