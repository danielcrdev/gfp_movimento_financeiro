# docker build -t image_gfp_movimento_financeiro -f . .
# docker run -d -p 5002:5002 --rm --name container_gfp_movimento_financeiro image_gfp_movimento_financeiro
#
FROM python:3
COPY . /gfp_movimento_financeiro
WORKDIR /gfp_movimento_financeiro
RUN pip install --no-cache-dir -r static/requirements.txt
CMD python -m flask run --host=0.0.0.0 --port=5002