from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import io
import re
import docx
import fitz  # PyMuPDF
from datetime import datetime, timedelta
import dateutil.parser
from typing import List, Optional
import os
from bson import ObjectId

# Initialize FastAPI app
app = FastAPI(
    title="Skillang CV Extractor API",
    version="1.0.0",
    description="CV data extraction and storage API for Skillang platform",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Configuration - Allow Skillang domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://skillang.com",
        "https://www.skillang.com", 
        "https://*.skillang.com",
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# MongoDB Configuration
MONGODB_URL = os.getenv("MONGODB_URL")
client = None
database = None
candidates_collection = None

# Response Models
class CandidateResponse(BaseModel):
    id: str
    name: str
    email: str
    phone: str
    age: Optional[int]
    skills: str
    education: str
    experience: str
    original_filename: str
    created_at: datetime
    
class ExtractResponse(BaseModel):
    success: bool
    message: str
    total_processed: int
    data: List[CandidateResponse]
    errors: List[str] = []

class SearchResponse(BaseModel):
    success: bool
    total_results: int
    data: List[CandidateResponse]
    
class StatsResponse(BaseModel):
    total_candidates: int
    recent_uploads: int
    top_skills: List[str]
    database_status: str

# Initialize MongoDB connection
async def init_db():
    global client, database, candidates_collection
    if MONGODB_URL:
        try:
            client = AsyncIOMotorClient(MONGODB_URL)
            database = client.cv_database
            candidates_collection = database.candidates
            # Test connection
            await client.admin.command('ping')
            print("✅ Connected to MongoDB Atlas")
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")

# CV Extraction Class
class CVExtractor:
    def extract_text_from_pdf(self, file_content: bytes) -> str:
        try:
            doc = fitz.open(stream=file_content, filetype="pdf")
            text = "\n".join([page.get_text() for page in doc])
            doc.close()
            return text
        except Exception as e:
            raise Exception(f"Error extracting PDF: {str(e)}")
    
    def extract_text_from_docx(self, file_content: bytes) -> str:
        try:
            doc = docx.Document(io.BytesIO(file_content))
            return "\n".join([para.text for para in doc.paragraphs])
        except Exception as e:
            raise Exception(f"Error extracting DOCX: {str(e)}")
    
    def extract_name(self, text: str) -> str:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Strategy 1: Look for explicit name patterns
        name_patterns = [
            r'(?:name|full\s*name)\s*[:\-]\s*([A-Za-z\s\.]+)',
            r'(?:candidate|applicant)\s*[:\-]\s*([A-Za-z\s\.]+)',
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                name = self.clean_name(match.group(1))
                if name and len(name.split()) >= 2:
                    return name
        
        # Strategy 2: First meaningful line with proper name format
        skip_patterns = [
            r'resume|curriculum|cv|contact|phone|email|address|objective|summary',
            r'^\d+[\s\-\(\)]+',  # Phone numbers
            r'@',  # Email addresses
            r'^[A-Z]{2,}\s',  # All caps headers
        ]
        
        for line in lines[:5]:
            if not any(re.search(pattern, line, re.IGNORECASE) for pattern in skip_patterns):
                # Check if it looks like a name (2+ words, proper case)
                if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+$', line.strip()):
                    return self.clean_name(line)
        
        # Strategy 3: Look for name-like patterns anywhere in first few lines
        for line in lines[:3]:
            words = line.split()
            if 2 <= len(words) <= 4:  # Names typically 2-4 words
                if all(word[0].isupper() and word[1:].islower() for word in words if word.isalpha()):
                    return self.clean_name(line)
        
        return lines[0][:50] if lines else "Name not found"
    
    def clean_name(self, name: str) -> str:
        # Remove special characters and extra spaces
        name = re.sub(r'[^\w\s\.]', '', name).strip()
        name = ' '.join(name.split())  # Remove extra spaces
        
        # Filter out common non-name words
        exclude_words = ['resume', 'cv', 'curriculum', 'vitae', 'contact', 'phone', 'email']
        words = [word for word in name.split() if word.lower() not in exclude_words]
        
        return ' '.join(words) if words else name
    
    def extract_email(self, text: str) -> str:
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(pattern, text)
        
        # Filter out common false positives
        valid_emails = [email for email in emails if not any(exclude in email.lower() 
                       for exclude in ['example.com', 'test.com', 'domain.com'])]
        
        return valid_emails[0] if valid_emails else ""
    
    def extract_phone(self, text: str) -> str:
        patterns = [
            r'\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # International
            r'\b\d{10}\b',  # Simple 10 digits
            r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',  # (123) 456-7890
            r'\+91[-.\s]?\d{10}',  # Indian format
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                phone = re.sub(r'[^\d+]', '', matches[0])
                if 10 <= len(phone) <= 15:  # Valid phone length
                    return phone
        return ""
    
    def extract_age(self, text: str) -> Optional[int]:
        # Direct age patterns
        age_patterns = [
            r'age\s*[:\-]?\s*(\d{1,2})',
            r'(\d{1,2})\s*years?\s*old',
            r'age\s*(\d{1,2})',
        ]
        
        for pattern in age_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                age = int(match.group(1))
                if 16 <= age <= 80:
                    return age
        
        # Date of birth patterns
        dob_patterns = [
            r'(?:dob|date\s*of\s*birth|born)\s*[:\-]?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
            r'(?:dob|born)\s*[:\-]?\s*(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{2,4})',
        ]
        
        for pattern in dob_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    date_str = match.group(1)
                    parsed_date = dateutil.parser.parse(date_str)
                    if 1940 <= parsed_date.year <= 2010:
                        return datetime.now().year - parsed_date.year
                except:
                    continue
        
        return None
    
    def extract_skills(self, text: str) -> str:
        # Look for skills section
        skills_patterns = [
            r'(?:technical\s*skills|skills|competencies|technologies)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,10})',
            r'(?:programming|software|tools)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,5})',
        ]
        
        for pattern in skills_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                skills_text = match.group(1)
                # Clean and split skills
                skills = re.split(r'[,;•\n\t]', skills_text)
                skills = [s.strip() for s in skills if s.strip() and len(s.strip()) > 2]
                
                # Filter out common non-skills
                exclude = ['years', 'experience', 'knowledge', 'familiar', 'working']
                skills = [s for s in skills if not any(e in s.lower() for e in exclude)]
                
                return ', '.join(skills[:15])  # Limit to 15 skills
        
        # Fallback: Look for common tech keywords
        tech_keywords = re.findall(r'\b(?:Python|Java|JavaScript|React|Node|SQL|AWS|Docker|Git|HTML|CSS|PHP|C\+\+|Angular|Vue|Django|Flask|Spring|Laravel|MongoDB|PostgreSQL|MySQL)\b', text, re.IGNORECASE)
        
        if tech_keywords:
            return ', '.join(list(set(tech_keywords))[:10])
        
        return ""
    
    def extract_education(self, text: str) -> str:
        edu_patterns = [
            r'(?:education|academic|qualification)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,5})',
            r'(bachelor|master|phd|degree|diploma|b\.tech|m\.tech|mba|bca|mca|be|me|ms|bs).*',
            r'(?:university|college|institute)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,3})',
        ]
        
        for pattern in edu_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                edu_text = match.group(1) if match.groups() else match.group(0)
                # Clean education text
                edu_text = ' '.join(edu_text.split()[:25])  # Limit length
                return edu_text.strip()
        
        return ""
    
    def extract_experience(self, text: str) -> str:
        exp_patterns = [
            r'(?:experience|work\s*experience|employment)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,8})',
            r'(\d+\+?\s*years?\s*(?:of\s*)?experience)',
            r'(?:worked|working)\s*(?:as|at)\s*([^\n]*)',
        ]
        
        for pattern in exp_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                exp_text = match.group(1) if match.groups() else match.group(0)
                exp_text = ' '.join(exp_text.split()[:30])  # Limit length
                return exp_text.strip()
        
        return ""
    
    def extract_all_details(self, text: str, filename: str) -> dict:
        return {
            'name': self.extract_name(text),
            'email': self.extract_email(text),
            'phone': self.extract_phone(text),
            'age': self.extract_age(text),
            'skills': self.extract_skills(text),
            'education': self.extract_education(text),
            'experience': self.extract_experience(text),
            'original_filename': filename,
            'created_at': datetime.now()
        }

# Initialize extractor
extractor = CVExtractor()

# API Endpoints
@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "Skillang CV Extractor API", 
        "version": "1.0.0", 
        "status": "active",
        "docs": "/docs"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    db_status = "connected" if candidates_collection else "disconnected"
    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": datetime.now().isoformat(),
        "mongodb_configured": MONGODB_URL is not None
    }

@app.post("/extract-and-store", response_model=ExtractResponse, tags=["CV Processing"])
async def extract_and_store_cvs(files: List[UploadFile] = File(...)):
    """
    Extract CV data from uploaded files and store in database
    
    - **files**: List of PDF or DOCX files (max 20)
    - Returns: Extracted candidate data with database storage
    """
    
    if not candidates_collection:
        raise HTTPException(status_code=500, detail="Database not connected. Check MongoDB configuration.")
    
    if len(files) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 files allowed per request")
    
    results = []
    errors = []
    
    for file in files:
        try:
            # Validate file type
            if not file.filename.lower().endswith(('.pdf', '.docx')):
                errors.append(f"{file.filename}: Only PDF and DOCX files are supported")
                continue
            
            # Validate file size (10MB limit)
            content = await file.read()
            if len(content) > 10 * 1024 * 1024:  # 10MB
                errors.append(f"{file.filename}: File too large (max 10MB)")
                continue
            
            # Extract text
            try:
                if file.filename.lower().endswith('.pdf'):
                    text = extractor.extract_text_from_pdf(content)
                else:
                    text = extractor.extract_text_from_docx(content)
            except Exception as e:
                errors.append(f"{file.filename}: {str(e)}")
                continue
            
            if not text.strip():
                errors.append(f"{file.filename}: No text content found")
                continue
            
            # Extract candidate details
            candidate_data = extractor.extract_all_details(text, file.filename)
            
            # Store in database
            result = await candidates_collection.insert_one(candidate_data)
            candidate_data['id'] = str(result.inserted_id)
            
            # Remove MongoDB ObjectId for response
            candidate_data.pop('_id', None)
            
            results.append(CandidateResponse(**candidate_data))
            
        except Exception as e:
            errors.append(f"{file.filename}: Unexpected error - {str(e)}")
    
    return ExtractResponse(
        success=True,
        message=f"Processed {len(results)} files successfully",
        total_processed=len(results),
        data=results,
        errors=errors
    )

@app.get("/candidates", response_model=SearchResponse, tags=["Candidate Management"])
async def get_candidates(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, le=100, description="Maximum records to return"),
    search: Optional[str] = Query(None, description="Search term for name, email, or skills")
):
    """Get all candidates with optional search and pagination"""
    
    if not candidates_collection:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    # Build query
    query = {}
    if search:
        query = {
            "$or": [
                {"name": {"$regex": search, "$options": "i"}},
                {"email": {"$regex": search, "$options": "i"}},
                {"skills": {"$regex": search, "$options": "i"}},
                {"education": {"$regex": search, "$options": "i"}}
            ]
        }
    
    # Get total count
    total_count = await candidates_collection.count_documents(query)
    
    # Get candidates
    cursor = candidates_collection.find(query).skip(skip).limit(limit).sort("created_at", -1)
    candidates = await cursor.to_list(length=limit)
    
    # Format response
    candidate_responses = []
    for candidate in candidates:
        candidate['id'] = str(candidate.pop('_id'))
        candidate_responses.append(CandidateResponse(**candidate))
    
    return SearchResponse(
        success=True,
        total_results=total_count,
        data=candidate_responses
    )

@app.get("/candidates/{candidate_id}", response_model=CandidateResponse, tags=["Candidate Management"])
async def get_candidate(candidate_id: str):
    """Get specific candidate by ID"""
    
    if not candidates_collection:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        candidate = await candidates_collection.find_one({"_id": ObjectId(candidate_id)})
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        candidate['id'] = str(candidate.pop('_id'))
        return CandidateResponse(**candidate)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid candidate ID")

@app.delete("/candidates/{candidate_id}", tags=["Candidate Management"])
async def delete_candidate(candidate_id: str):
    """Delete a candidate"""
    
    if not candidates_collection:
        raise HTTPException(status_code=500, detail="Database not connected")
    
    try:
        result = await candidates_collection.delete_one({"_id": ObjectId(candidate_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        return {"success": True, "message": "Candidate deleted successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid candidate ID")

@app.get("/stats", response_model=StatsResponse, tags=["Analytics"])
async def get_stats():
    """Get database and extraction statistics"""
    
    if not candidates_collection:
        return StatsResponse(
            total_candidates=0,
            recent_uploads=0,
            top_skills=[],
            database_status="disconnected"
        )
    
    try:
        # Total candidates
        total_candidates = await candidates_collection.count_documents({})
        
        # Recent uploads (last 7 days)
        week_ago = datetime.now() - timedelta(days=7)
        recent_uploads = await candidates_collection.count_documents({
            "created_at": {"$gte": week_ago}
        })
        
        # Top skills (aggregation)
        pipeline = [
            {"$match": {"skills": {"$ne": ""}}},
            {"$project": {"skills_array": {"$split": ["$skills", ","]}}},
            {"$unwind": "$skills_array"},
            {"$project": {"skill": {"$trim": {"input": "$skills_array"}}}},
            {"$group": {"_id": "$skill", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_skills_cursor = candidates_collection.aggregate(pipeline)
        top_skills_data = await top_skills_cursor.to_list(length=10)
        top_skills = [item["_id"] for item in top_skills_data if item["_id"]]
        
        return StatsResponse(
            total_candidates=total_candidates,
            recent_uploads=recent_uploads,
            top_skills=top_skills,
            database_status="connected"
        )
        
    except Exception as e:
        return StatsResponse(
            total_candidates=0,
            recent_uploads=0,
            top_skills=[],
            database_status="error"
        )

# Handle preflight requests
@app.options("/{full_path:path}")
async def options_handler():
    return JSONResponse(content={}, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    })

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"success": False, "message": f"Internal server error: {str(exc)}"}
    )

# Add this debug endpoint to your main.py temporarily

@app.get("/debug-mongo", tags=["Debug"])
async def debug_mongo_connection():
    """Debug MongoDB connection with detailed error info"""
    
    mongodb_url = os.getenv("MONGODB_URL")
    
    debug_info = {
        "mongodb_url_exists": mongodb_url is not None,
        "mongodb_url_length": len(mongodb_url) if mongodb_url else 0,
        "mongodb_url_preview": mongodb_url[:50] + "..." if mongodb_url else None,
        "connection_attempts": []
    }
    
    if not mongodb_url:
        debug_info["error"] = "MONGODB_URL environment variable not found"
        return debug_info
    
    # Test 1: Basic connection
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        test_client = AsyncIOMotorClient(mongodb_url, serverSelectionTimeoutMS=5000)
        await test_client.admin.command('ping')
        debug_info["connection_attempts"].append({"test": "basic_ping", "status": "success"})
        test_client.close()
    except Exception as e:
        debug_info["connection_attempts"].append({
            "test": "basic_ping", 
            "status": "failed",
            "error": str(e),
            "error_type": type(e).__name__
        })
    
    # Test 2: Database access
    try:
        test_client = AsyncIOMotorClient(mongodb_url, serverSelectionTimeoutMS=5000)
        test_db = test_client.get_database()  # Get default database
        collections = await test_db.list_collection_names()
        debug_info["connection_attempts"].append({
            "test": "database_access", 
            "status": "success",
            "collections_found": len(collections)
        })
        test_client.close()
    except Exception as e:
        debug_info["connection_attempts"].append({
            "test": "database_access",
            "status": "failed", 
            "error": str(e),
            "error_type": type(e).__name__
        })
    
    # Test 3: Specific database access
    try:
        test_client = AsyncIOMotorClient(mongodb_url, serverSelectionTimeoutMS=5000)
        test_db = test_client.skillang_email_scheduler  # Try the existing database
        collections = await test_db.list_collection_names()
        debug_info["connection_attempts"].append({
            "test": "specific_database", 
            "status": "success",
            "database": "skillang_email_scheduler",
            "collections": collections
        })
        test_client.close()
    except Exception as e:
        debug_info["connection_attempts"].append({
            "test": "specific_database",
            "status": "failed",
            "database": "skillang_email_scheduler", 
            "error": str(e),
            "error_type": type(e).__name__
        })
    
    return debug_info

# For Vercel
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)