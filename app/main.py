from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import requests
import base64
from io import BytesIO
from typing import List, Optional
import gc
import threading
import fitz  # PyMuPDF
import io
from PIL import Image
import tempfile
import os

# Make sure to import this middleware
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="PDF to Images Converter - PyMuPDF Optimized", version="3.0.0")

# Configuration
MAX_PAGES_PER_REQUEST = 100
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for download
DEFAULT_DPI = 150

# Thread-local storage
thread_local = threading.local()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PDFRequest(BaseModel):
    page_range: str  # Format: "1-3,5,7-9"
    url: HttpUrl
    dpi: int = DEFAULT_DPI
    quality: int = 85  # JPEG quality (1-100)
    use_jpeg: bool = True

class PDFResponse(BaseModel):
    images: List[str]  # List of base64 encoded images
    total_pages_processed: int
    pages: List[int]   # Actual page numbers processed
    file_size: int
    memory_used: str

def get_session():
    """Get thread-local requests session"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def parse_page_range(page_range: str, max_pages: int) -> List[int]:
    """Parse page range string with validation"""
    pages = set()
    
    if page_range.lower() == 'all':
        return list(range(1, max_pages + 1))
    
    parts = page_range.replace(' ', '').split(',')
    
    for part in parts:
        if '-' in part:
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
            try:
                page = int(part)
            except ValueError:
                raise ValueError(f"Invalid page number: {part}")
            
            if page < 1 or page > max_pages:
                raise ValueError(f"Page {page} out of valid bounds (1-{max_pages})")
            
            pages.add(page)
    
    return sorted(pages)

def download_pdf_to_tempfile(url: str) -> str:
    """Download PDF to temporary file to avoid memory issues"""
    session = get_session()
    
    with session.get(url, stream=True) as response:
        response.raise_for_status()
        
        content_length = response.headers.get('Content-Length')
        if content_length and int(content_length) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400, 
                detail=f"PDF file too large (max {MAX_FILE_SIZE // (1024*1024)}MB)"
            )
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        
        try:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    temp_file.write(chunk)
            temp_file.close()
            return temp_file.name
        except Exception as e:
            os.unlink(temp_file.name)
            raise e

def get_pdf_page_count(pdf_path: str) -> int:
    """Get PDF page count using PyMuPDF"""
    try:
        doc = fitz.open(pdf_path)
        page_count = len(doc)
        doc.close()
        return page_count
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid PDF file: {str(e)}")

def process_single_page(pdf_path: str, page_num: int, dpi: int, quality: int, use_jpeg: bool) -> Optional[str]:
    """Process a single page using PyMuPDF"""
    doc = None
    try:
        # Open PDF and load only the specific page
        doc = fitz.open(pdf_path)
        
        # PyMuPDF uses 0-indexed pages
        page_index = page_num - 1
        
        if page_index >= len(doc) or page_index < 0:
            return None
        
        # Get the specific page
        page = doc.load_page(page_index)
        
        # Create a matrix for the desired DPI
        mat = fitz.Matrix(dpi / 72, dpi / 72)  # 72 is the default DPI
        
        # Render page to an image
        pix = page.get_pixmap(matrix=mat, alpha=False)
        
        # Convert to PIL Image for compression
        img_data = pix.tobytes("ppm")
        pil_img = Image.open(io.BytesIO(img_data))
        
        # Convert to RGB if necessary (remove alpha channel)
        if pil_img.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', pil_img.size, (255, 255, 255))
            background.paste(pil_img, mask=pil_img.split()[-1])
            pil_img = background
        elif pil_img.mode != 'RGB':
            pil_img = pil_img.convert('RGB')
        
        # Save to bytes with compression
        output_buffer = BytesIO()
        
        if use_jpeg:
            pil_img.save(output_buffer, format='JPEG', quality=quality, optimize=True)
        else:
            pil_img.save(output_buffer, format='PNG', optimize=True)
        
        # Convert to base64
        img_base64 = base64.b64encode(output_buffer.getvalue()).decode('utf-8')
        
        return img_base64
        
    except Exception as e:
        print(f"Error processing page {page_num}: {str(e)}")
        return None
    finally:
        if doc:
            doc.close()

def process_page_range_low_memory(pdf_path: str, page_numbers: List[int], dpi: int, quality: int, use_jpeg: bool) -> List[str]:
    """Process pages with minimal memory usage by processing one at a time"""
    base64_images = []
    successful_pages = []
    
    for page_num in page_numbers:
        # Process one page at a time
        result = process_single_page(pdf_path, page_num, dpi, quality, use_jpeg)
        
        if result is not None:
            base64_images.append(result)
            successful_pages.append(page_num)
        
        # Force garbage collection after each page
        gc.collect()
    
    return base64_images, successful_pages

@app.post("/convert-pdf", response_model=PDFResponse)
async def convert_pdf_to_images(request: PDFRequest):
    temp_file_path = None
    try:
        # Download PDF to temporary file
        temp_file_path = download_pdf_to_tempfile(str(request.url))
        file_size = os.path.getsize(temp_file_path)
        
        # Get page count
        total_pages = get_pdf_page_count(temp_file_path)
        
        # Parse page range
        requested_pages = parse_page_range(request.page_range, total_pages)
        
        if not requested_pages:
            raise HTTPException(status_code=400, detail="No valid pages in the specified range")
        
        # Limit number of pages per request
        if len(requested_pages) > MAX_PAGES_PER_REQUEST:
            raise HTTPException(
                status_code=400, 
                detail=f"Too many pages requested (max {MAX_PAGES_PER_REQUEST})"
            )
        
        # Validate DPI
        if request.dpi > 600:
            raise HTTPException(status_code=400, detail="DPI too high (max 600)")
        if request.dpi < 50:
            raise HTTPException(status_code=400, detail="DPI too low (min 50)")
        
        # Process only the requested pages
        base64_images, successful_pages = process_page_range_low_memory(
            temp_file_path, 
            requested_pages, 
            request.dpi, 
            request.quality, 
            request.use_jpeg
        )
        
        if not base64_images:
            raise HTTPException(status_code=500, detail="Failed to convert any pages")
        
        # Calculate memory usage (rough estimate)
        total_image_size = sum(len(img.encode('utf-8')) for img in base64_images)
        memory_used = total_image_size / (1024 * 1024)  # MB
        
        return PDFResponse(
            images=base64_images,
            total_pages_processed=len(base64_images),
            pages=successful_pages,
            file_size=file_size,
            memory_used=f"{memory_used:.1f} MB"
        )
        
    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to download PDF: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass
        gc.collect()

@app.post("/convert-pdf-direct")
async def convert_pdf_direct(url: HttpUrl, pages: str = "1", dpi: int = DEFAULT_DPI):
    """Simplified endpoint for direct use"""
    request = PDFRequest(url=url, page_range=pages, dpi=dpi)
    return await convert_pdf_to_images(request)

@app.get("/pdf-info")
async def get_pdf_info(url: HttpUrl):
    """Get PDF information without processing pages"""
    temp_file_path = None
    try:
        temp_file_path = download_pdf_to_tempfile(str(url))
        file_size = os.path.getsize(temp_file_path)
        total_pages = get_pdf_page_count(temp_file_path)
        
        return {
            "total_pages": total_pages,
            "file_size_bytes": file_size,
            "file_size_mb": f"{file_size / (1024 * 1024):.2f}",
            "max_pages_per_request": MAX_PAGES_PER_REQUEST,
            "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass

@app.get("/preview-page")
async def preview_page(url: HttpUrl, page: int = 1, dpi: int = 100):
    """Preview a single page with low DPI for quick preview"""
    temp_file_path = None
    try:
        temp_file_path = download_pdf_to_tempfile(str(url))
        total_pages = get_pdf_page_count(temp_file_path)
        
        if page < 1 or page > total_pages:
            raise HTTPException(status_code=400, detail=f"Page {page} out of range (1-{total_pages})")
        
        # Process only the requested page
        images, successful_pages = process_page_range_low_memory(
            temp_file_path, [page], dpi, 75, True
        )
        
        if not images:
            raise HTTPException(status_code=500, detail="Failed to convert page")
        
        return {
            "image": images[0],
            "page": successful_pages[0],
            "total_pages": total_pages,
            "format": "jpeg"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass

@app.get("/")
async def root():
    return {
        "message": "PyMuPDF PDF to Images Converter API", 
        "version": "3.0.0",
        "optimizations": [
            "Uses PyMuPDF for efficient PDF processing",
            "Processes only requested page range",
            "Low memory usage with temporary files",
            "Progressive garbage collection",
            "Streaming downloads with chunking"
        ],
        "endpoints": {
            "POST /convert-pdf": "Convert PDF pages to images",
            "GET /pdf-info": "Get PDF metadata",
            "GET /preview-page": "Quick preview of a single page"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "engine": "PyMuPDF", "memory_optimized": True}

# Cleanup on shutdown
@app.on_event("shutdown")
def shutdown_event():
    if hasattr(thread_local, "session"):
        thread_local.session.close()
