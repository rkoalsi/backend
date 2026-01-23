from fastapi import APIRouter, HTTPException, Response
from ..config.root import get_database
from bson import ObjectId
import os, requests
from .helpers import get_access_token

router = APIRouter()

db = get_database()

org_id = os.getenv("ORG_ID")
CREDITNOTE_PDF_URL = os.getenv("CREDITNOTE_PDF_URL")


@router.get("/download_pdf/{credit_note_id}")
async def download_pdf(credit_note_id: str = ""):
    try:
        # Check if the credit note exists in the database
        credit_note = db.credit_notes.find_one({"_id": ObjectId(credit_note_id)})
        if credit_note is None:
            raise HTTPException(status_code=404, detail="Credit Note Not Found")

        # Get the creditnote_id and make the request to Zoho
        zoho_creditnote_id = credit_note.get("creditnote_id", "")
        headers = {"Authorization": f"Zoho-oauthtoken {get_access_token('books')}"}
        response = requests.get(
            url=CREDITNOTE_PDF_URL.format(org_id=org_id, creditnote_id=zoho_creditnote_id),
            headers=headers,
            allow_redirects=False,
        )

        # Check if the response from Zoho is successful (200)
        if response.status_code == 200:
            # Return the PDF content
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename=credit_note_{zoho_creditnote_id}.pdf"
                },
            )
        elif response.status_code == 307:
            raise HTTPException(
                status_code=307,
                detail="Redirect encountered. Check Zoho endpoint or token.",
            )
        else:
            # Raise an exception if Zoho's API returns an error
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch PDF: {response.text}",
            )

    except HTTPException as e:
        print(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
