from http.server import BaseHTTPRequestHandler, HTTPServer
import json, os
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            body = json.dumps({'status':'ok'}).encode()
            self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *_): pass
def run(): HTTPServer(('0.0.0.0', int(os.getenv('PORT','8000'))), Handler).serve_forever()
if __name__ == '__main__': run()
