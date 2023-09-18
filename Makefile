run: 
	docker build -t italy-housing .
	docker run -it --rm -p 8888:8888 italy-housing
	