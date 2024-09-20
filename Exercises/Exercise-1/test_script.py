import pytest
import aiohttp
import asyncio
from aioresponses import aioresponses
from main2 import extract_file  # Replace 'your_script' with the actual name of your script

import io
import zipfile
import os

@pytest.mark.asyncio
async def test_extract_file_success(tmpdir):
    # Mock URL and output path
    test_url = "https://test.com/test.zip"
    output_path = tmpdir.mkdir("downloads")
    
    # Create a dummy zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('test_file.txt', 'This is a test file.')

    # Mock aiohttp response
    with aioresponses() as mock_response:
        mock_response.get(test_url, body=zip_buffer.getvalue(), status=200)
        
        # Execute the extract_file function
        async with aiohttp.ClientSession() as session:
            await extract_file(session, test_url, str(output_path))
    
    # Verify the file was extracted
    extracted_file = output_path.join("test_file.txt")
    assert extracted_file.exists(), "The file was not extracted properly"
    assert extracted_file.read() == "This is a test file.", "The file contents are incorrect"


@pytest.mark.asyncio
async def test_extract_file_request_fail(tmpdir, caplog):
    # Mock URL and output path
    test_url = "https://test.com/test.zip"
    output_path = tmpdir.mkdir("downloads")

    # Mock aiohttp to simulate a failed request (e.g., 404 error)
    with aioresponses() as mock_response:
        mock_response.get(test_url, status=404)
        
        # Execute the extract_file function
        async with aiohttp.ClientSession() as session:
            await extract_file(session, test_url, str(output_path))
    
    # Check if the appropriate error message was logged
    assert any("Request failed" in message for message in caplog.text), "The error was not logged"


@pytest.mark.asyncio
async def test_extract_file_bad_zip(tmpdir, caplog):
    # Mock URL and output path
    test_url = "https://test.com/test.zip"
    output_path = tmpdir.mkdir("downloads")
    
    # Simulate an invalid zip file in memory
    bad_zip_buffer = io.BytesIO(b"Invalid zip content")
    
    # Mock aiohttp response with bad zip content
    with aioresponses() as mock_response:
        mock_response.get(test_url, body=bad_zip_buffer.getvalue(), status=200)
        
        # Execute the extract_file function
        async with aiohttp.ClientSession() as session:
            await extract_file(session, test_url, str(output_path))
    
    # Check if the appropriate error message was logged
    assert any("Failed to unzip file" in message for message in caplog.text), "The error was not logged"

