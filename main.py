from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import fitz  # PyMuPDF
import io
import requests
from PIL import Image
import asyncio
import aiohttp

app = FastAPI()

async def download_pdf(pdf_url: str) -> bytes:
    """Download PDF from URL asynchronously"""
    async with aiohttp.ClientSession() as session:
        async with session.get(pdf_url) as response:
            if response.status != 200:
                raise HTTPException(status_code=400, detail="Failed to download PDF")
            return await response.read()

def convert_pdf_page_to_image(pdf_data: bytes, page_num: int, dpi: int = 200) -> io.BytesIO:
    """Convert a single PDF page to high-quality image"""
    # Open PDF document from memory
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    
    try:
        if page_num >= len(pdf_document):
            raise HTTPException(status_code=404, detail=f"Page {page_num} not found")
        
        # Get the page
        page = pdf_document[page_num]
        
        # Create high-resolution matrix for rendering
        zoom = dpi / 72  # PDF default is 72 DPI
        mat = fitz.Matrix(zoom, zoom)
        
        # Render page to pixmap
        pix = page.get_pixmap(matrix=mat)
        
        # Convert to PIL Image for quality control
        img_data = pix.tobytes("ppm")
        img = Image.open(io.BytesIO(img_data))
        
        # Convert to RGB if necessary (remove alpha channel)
        if img.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1])
            img = background
        
        # Save as high-quality JPEG to bytes buffer
        output_buffer = io.BytesIO()
        img.save(output_buffer, format='JPEG', quality=95, optimize=True)
        output_buffer.seek(0)
        
        return output_buffer
    finally:
        pdf_document.close()

@app.get("/pdf-images/{pdf_url:path}")
async def get_pdf_images_info(pdf_url: str):
    """
    Returns PDF metadata and individual image URLs
    """
    pdf_data = await download_pdf(pdf_url)
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    page_count = len(pdf_document)
    pdf_document.close()
    
    # Return URLs to individual image endpoints
    return {
        "page_count": page_count,
        "images": [
            {
                "page_number": i,
                "url": f"/get-image/{pdf_url}?page={i}",
                "download_url": f"/get-image/{pdf_url}?page={i}&download=true"
            }
            for i in range(page_count)
        ]
    }

@app.get("/get-image/{pdf_url:path}")
async def get_single_image(pdf_url: str, page: int = 0, dpi: int = 200, download: bool = False):
    """Get single PDF page as image stream"""
    pdf_data = await download_pdf(pdf_url)
    image_buffer = convert_pdf_page_to_image(pdf_data, page, dpi)
    
    filename = f"page_{page+1}.jpg"
    headers = {"Content-Disposition": f"{'attachment' if download else 'inline'}; filename={filename}"}
    
    return StreamingResponse(
        image_buffer,
        media_type="image/jpeg",
        headers=headers
    )
