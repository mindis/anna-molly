#!/usr/bin/env make

HOME = ./
NAME = tm-cpc-engine
TEST = ./test/

.PHONY:clean test docs

init:
	pip install -r requirements.txt

clean:
	-rm $(HOME)*.pyc

test:
	make -C $(TEST)
