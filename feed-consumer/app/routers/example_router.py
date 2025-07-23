from fastapi import APIRouter

router = APIRouter()

@router.get("/example")
def example_endpoint():
    return {"message": "This is an example endpoint"}