COMMIT ?= $(shell git rev-parse --verify -q HEAD || echo unknown)

my-app: lab/app/main.sh
	sed 's/@COMMIT@/$(COMMIT)/' lab/app/main.sh > my-app
	chmod +x my-app

clean:
	rm -f my-app

.PHONY: clean
