all : main

compile :
	javac -d build -cp build/ src/*.java
	

main: compile
	java -classpath build Main data.txt

clean :
	@rm -rf build