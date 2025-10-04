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

@app.get("/pdf-to-images/{pdf_url:path}")
async def convert_pdf_to_images(pdf_url: str, dpi: int = 200):
    """
    Convert PDF to array of image streams
    - pdf_url: URL of the PDF to convert
    - dpi: Resolution quality (150-300 recommended)
    """
    try:
        # Download PDF
        pdf_data = await download_pdf(pdf_url)
        
        # Get basic PDF info
        pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
        page_count = len(pdf_document)
        pdf_document.close()
        
        # Convert each page to image
        image_streams = []
        for page_num in range(page_count):
            image_buffer = convert_pdf_page_to_image(pdf_data, page_num, dpi)
            image_streams.append(image_buffer)
        
        return {
            "page_count": page_count,
            "images": [
                {"page_number": i, "stream_available": True} 
                for i in range(len(image_streams))
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/get-image/{pdf_url:path}")
async def get_single_image(pdf_url: str, page: int = 0, dpi: int = 200):
    """Get single PDF page as image stream"""
    pdf_data = await download_pdf(pdf_url)
    image_buffer = convert_pdf_page_to_image(pdf_data, page, dpi)
    
    return StreamingResponse(
        image_buffer,
        media_type="image/jpeg",
        headers={"Content-Disposition": f"inline; filename=page_{page}.jpg"}
    )
