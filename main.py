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

# MongoDB Configuration - Optimized for Serverless
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

# Optimized MongoDB connection for Vercel/Serverless
async def get_database():
    """Get database connection with serverless optimization"""
    global client, database, candidates_collection
    
    if not MONGODB_URL:
        print("❌ MONGODB_URL environment variable not set")
        return None, None
    
    # If connection exists and is healthy, reuse it
    if client is not None:
        try:
            # Quick ping to check if connection is alive
            await client.admin.command('ping')
            return database, candidates_collection
        except Exception as e:
            print(f"⚠️ Existing connection failed, reconnecting: {e}")
            client = None
    
    try:
        print(f"🔄 Creating new MongoDB connection...")
        
        # Serverless-optimized connection settings
        client = AsyncIOMotorClient(
            MONGODB_URL,
            # Optimized for serverless/Vercel
            serverSelectionTimeoutMS=3000,    # Reduced timeout
            connectTimeoutMS=3000,            # Reduced timeout
            socketTimeoutMS=3000,             # Socket timeout
            maxPoolSize=1,                    # Single connection for serverless
            minPoolSize=0,                    # No minimum pool
            maxIdleTimeMS=10000,              # Close idle connections quickly
            retryWrites=True,                 # Enable retry writes
            w="majority"                      # Write concern
        )
        
        # Test connection immediately
        await client.admin.command('ping')
        print("✅ MongoDB connection successful")
        
        # Initialize database and collection
        database = client.cv_database
        candidates_collection = database.candidates
        
        # Verify collection access
        count = await candidates_collection.count_documents({})
        print(f"✅ Database ready: cv_database (documents: {count})")
        
        return database, candidates_collection
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        client = None
        database = None
        candidates_collection = None
        return None, None

# Helper function to ensure database connection
async def ensure_db_connection():
    """Ensure database connection before operations"""
    db, collection = await get_database()
    if collection is None:
        raise HTTPException(status_code=500, detail="Database connection failed. Please check MongoDB configuration.")
    return db, collection

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
        
        # Strategy 2: Look for name patterns that appear with job titles
        job_title_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*\n\s*(?:Intensive Care Nurse|Software Engineer|Manager|Developer|Analyst|Consultant|Specialist|Executive|Officer|Assistant|Coordinator|Director|Lead|Senior|Junior)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*(?:Intensive Care Nurse|Software Engineer|Manager|Developer|Analyst|Consultant|Specialist|Executive|Officer|Assistant|Coordinator|Director|Lead|Senior|Junior)',
        ]
        
        for pattern in job_title_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                name = self.clean_name(match.group(1))
                if name and len(name.split()) >= 2:
                    return name
        
        # Strategy 3: Skip common headers and look for proper names
        skip_patterns = [
            r'resume|curriculum|cv|contact|phone|email|address|objective|summary|personal\s*information',
            r'^\d+[\s\-\(\)]+',  # Phone numbers
            r'@',  # Email addresses
            r'^[A-Z\s]{3,}$',  # All caps headers like "PERSONAL INFORMATION"
            r'key\s*skills|work\s*experience|education|profile|about',
            r'mobile|total\s*work|city|country|hobbies|languages'
        ]
        
        potential_names = []
        
        for i, line in enumerate(lines[:15]):  # Check first 15 lines
            # Skip if matches any skip pattern
            if any(re.search(pattern, line, re.IGNORECASE) for pattern in skip_patterns):
                continue
            
            # Look for lines that could be names (2-4 words, proper case)
            words = line.split()
            if 2 <= len(words) <= 4:
                # Check if it looks like a proper name
                if all(word[0].isupper() and len(word) > 1 and word[1:].islower() for word in words if word.isalpha()):
                    # Additional check: make sure it's not a common non-name phrase
                    non_name_words = ['Email', 'Mobile', 'Total', 'Years', 'Months', 'City', 'Country', 'English', 'Hindi']
                    if not any(word in non_name_words for word in words):
                        potential_names.append((i, self.clean_name(line)))
        
        # Return the first valid potential name
        if potential_names:
            return potential_names[0][1]
        
        # Strategy 4: Look anywhere in text for name-like patterns
        name_regex = r'\b([A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\b'
        matches = re.findall(name_regex, text)
        
        for match in matches:
            if not any(re.search(pattern, match, re.IGNORECASE) for pattern in skip_patterns):
                return self.clean_name(match)
        
        # Fallback: return first non-empty line (but limit length)
        return lines[0][:50] if lines else "Name not found"
    
    def clean_name(self, name: str) -> str:
        # Remove special characters and extra spaces
        name = re.sub(r'[^\w\s\.]', '', name).strip()
        name = ' '.join(name.split())  # Remove extra spaces
        
        # Filter out common non-name words
        exclude_words = ['resume', 'cv', 'curriculum', 'vitae', 'contact', 'phone', 'email', 'mobile', 'total', 'work']
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
            r'\(\+91\)\s*\d{10}',  # (+91) 9958630825 format
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                phone = re.sub(r'[^\d+]', '', matches[0])
                if 10 <= len(phone) <= 15:  # Valid phone length
                    return phone
        return ""
    
    def extract_age(self, text: str) -> Optional[int]:
        # Strategy 1: Direct age patterns
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
        
        # Strategy 2: Date of birth patterns
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
        # Strategy 1: Look for explicit skills sections
        skills_patterns = [
            r'(?:key\s*skills|technical\s*skills|skills|competencies|technologies)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,15})',
            r'(?:programming|software|tools)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,8})',
        ]
        
        for pattern in skills_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                skills_text = match.group(1)
                # Clean and split skills
                skills = re.split(r'[,;•\n\t]', skills_text)
                skills = [s.strip() for s in skills if s.strip() and len(s.strip()) > 1]
                
                # Filter out common non-skills
                exclude = ['years', 'experience', 'knowledge', 'familiar', 'working', 'other', 'personal', 'details']
                skills = [s for s in skills if not any(e in s.lower() for e in exclude) and len(s) < 50]
                
                if skills:
                    return ', '.join(skills[:15])  # Limit to 15 skills
        
        # Strategy 2: Look for medical/nursing specific skills
        medical_keywords = re.findall(r'\b(?:ICU|Medical|Surgical|Nursing|BSc|BLS|ECMO|CRRT|Ventilator|Cardiac|Intensive Care|Emergency|Critical Care|Patient Care)\b', text, re.IGNORECASE)
        
        # Strategy 3: Look for general tech keywords
        tech_keywords = re.findall(r'\b(?:Python|Java|JavaScript|React|Node|SQL|AWS|Docker|Git|HTML|CSS|PHP|C\+\+|Angular|Vue|Django|Flask|Spring|Laravel|MongoDB|PostgreSQL|MySQL)\b', text, re.IGNORECASE)
        
        all_keywords = medical_keywords + tech_keywords
        if all_keywords:
            # Remove duplicates and limit
            unique_skills = list(dict.fromkeys(all_keywords))  # Preserves order while removing duplicates
            return ', '.join(unique_skills[:10])
        
        return ""
    
    def extract_education(self, text: str) -> str:
        # Look for education section
        edu_patterns = [
            r'(?:education|academic|qualification)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,8})',
            r'(b\.?sc|bachelor|master|phd|degree|diploma|b\.?tech|m\.?tech|mba|bca|mca|be|me|ms|bs).*?(?:university|college|institute|school)',
            r'(?:university|college|institute)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,5})',
        ]
        
        for pattern in edu_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                edu_text = match.group(1) if match.groups() else match.group(0)
                # Clean education text
                edu_text = ' '.join(edu_text.split()[:30])  # Limit length
                edu_text = re.sub(r'\d{4}', '', edu_text)  # Remove years for cleaner display
                return edu_text.strip()
        
        return ""
    
    def extract_experience(self, text: str) -> str:
        # Look for experience patterns
        exp_patterns = [
            r'(?:total\s*work\s*experience|work\s*experience|experience)[:\-]?\s*([^\n]*(?:\n[^\n]*){0,3})',
            r'(\d+\+?\s*years?\s*(?:\d+\s*months?)?\s*(?:of\s*)?experience)',
            r'(?:worked|working)\s*(?:as|at)\s*([^\n]*)',
            r'(\d+\s*years?\s*\d+\s*months?)',  # Pattern like "7 Years 8 Months"
        ]
        
        for pattern in exp_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                exp_text = match.group(1) if match.groups() else match.group(0)
                exp_text = ' '.join(exp_text.split()[:25])  # Limit length
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
    db_status = "disconnected"
    error_detail = None
    
    if not MONGODB_URL:
        error_detail = "MONGODB_URL not configured"
    else:
        try:
            # Test connection
            db, collection = await get_database()
            if db is not None and collection is not None:
                # Test collection access
                await collection.count_documents({})
                db_status = "connected"
            else:
                error_detail = "Database connection failed"
        except Exception as e:
            error_detail = f"Connection test failed: {str(e)}"
    
    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": datetime.now().isoformat(),
        "mongodb_configured": MONGODB_URL is not None,
        "environment": "vercel" if os.getenv("VERCEL") else "local",
        "error_detail": error_detail
    }

@app.get("/debug-mongo", tags=["Debug"])
async def debug_mongo_connection():
    """Debug MongoDB connection with detailed error info"""
    
    mongodb_url = os.getenv("MONGODB_URL")
    
    debug_info = {
        "mongodb_url_exists": mongodb_url is not None,
        "mongodb_url_length": len(mongodb_url) if mongodb_url else 0,
        "mongodb_url_preview": mongodb_url[:50] + "..." if mongodb_url else None,
        "environment": "vercel" if os.getenv("VERCEL") else "local",
        "vercel_env": os.getenv("VERCEL_ENV", "unknown"),
        "connection_attempts": []
    }
    
    if not mongodb_url:
        debug_info["error"] = "MONGODB_URL environment variable not found"
        return debug_info
    
    # Test connection with serverless settings
    try:
        test_client = AsyncIOMotorClient(
            mongodb_url, 
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=3000,
            maxPoolSize=1
        )
        await test_client.admin.command('ping')
        debug_info["connection_attempts"].append({"test": "serverless_ping", "status": "success"})
        
        # Test database access
        test_db = test_client.cv_database
        collections = await test_db.list_collection_names()
        debug_info["connection_attempts"].append({
            "test": "cv_database", 
            "status": "success",
            "collections": collections
        })
        
        test_client.close()
        
    except Exception as e:
        debug_info["connection_attempts"].append({
            "test": "serverless_connection", 
            "status": "failed",
            "error": str(e),
            "error_type": type(e).__name__
        })
    
    return debug_info

@app.post("/extract-and-store", response_model=ExtractResponse, tags=["CV Processing"])
async def extract_and_store_cvs(files: List[UploadFile] = File(...)):
    """Extract CV data from uploaded files and store in database"""
    
    # Ensure database connection
    db, collection = await ensure_db_connection()
    
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
            result = await collection.insert_one(candidate_data)
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
    
    # Ensure database connection
    db, collection = await ensure_db_connection()
    
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
    total_count = await collection.count_documents(query)
    
    # Get candidates
    cursor = collection.find(query).skip(skip).limit(limit).sort("created_at", -1)
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
    
    # Ensure database connection
    db, collection = await ensure_db_connection()
    
    try:
        candidate = await collection.find_one({"_id": ObjectId(candidate_id)})
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        candidate['id'] = str(candidate.pop('_id'))
        return CandidateResponse(**candidate)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid candidate ID")

@app.delete("/candidates/{candidate_id}", tags=["Candidate Management"])
async def delete_candidate(candidate_id: str):
    """Delete a candidate"""
    
    # Ensure database connection
    db, collection = await ensure_db_connection()
    
    try:
        result = await collection.delete_one({"_id": ObjectId(candidate_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        return {"success": True, "message": "Candidate deleted successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid candidate ID")

@app.get("/stats", response_model=StatsResponse, tags=["Analytics"])
async def get_stats():
    """Get database and extraction statistics"""
    
    try:
        # Ensure database connection
        db, collection = await ensure_db_connection()
        
        # Total candidates
        total_candidates = await collection.count_documents({})
        
        # Recent uploads (last 7 days)
        week_ago = datetime.now() - timedelta(days=7)
        recent_uploads = await collection.count_documents({
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
        
        top_skills_cursor = collection.aggregate(pipeline)
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

# For Vercel - No startup event needed, connections are made on-demand
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)