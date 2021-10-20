from flask import Flask, request
import json
from servises import s3_upload_file, mediaconvert_create_job, invalidate_cloudfront

app = Flask(__name__)
app.config.from_json("configs/app_config.json")


@app.route('/upload_video', methods=['POST'])
def upload_video():
    file = request.files.get('video')
    if file:
        file_name = s3_upload_file(file)
        mc_job = mediaconvert_create_job(file_name)
        if isinstance(mc_job, str):
            return f"Error processing MediaConvert job: {mc_job}", 422
        
        domain = invalidate_cloudfront()

        video_url = f"https://{domain}/{file_name}out.mp4"
        return video_url, 200

    return 'File with name "video" none found', 400
