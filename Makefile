.PHONY: help wire conf ent build api openapi init all test test-short vet

# generate protobuf api go code
api:
	cd api && \
	buf generate

# Run go test across every module that has a go.mod.
test:
	@for dir in $$(find . -name go.mod -not -path './.git/*' -not -path './.zcode/*' | sed 's,/go.mod$$,,' | sort); do \
		echo "==> testing $$dir"; \
		(cd "$$dir" && go test ./...) || exit 1; \
	done

# Run only hermetic tests (integration tests skip themselves under -short).
test-short:
	@for dir in $$(find . -name go.mod -not -path './.git/*' -not -path './.zcode/*' | sed 's,/go.mod$$,,' | sort); do \
		echo "==> testing (short) $$dir"; \
		(cd "$$dir" && go test -short ./...) || exit 1; \
	done

# Run go vet across every module.
vet:
	@for dir in $$(find . -name go.mod -not -path './.git/*' -not -path './.zcode/*' | sed 's,/go.mod$$,,' | sort); do \
		echo "==> vetting $$dir"; \
		(cd "$$dir" && go vet ./...) || exit 1; \
	done

# show help
help:
	@echo ""
	@echo "Usage:"
	@echo " make [target]"
	@echo ""
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-_0-9]+:/ { \
	helpMessage = match(lastLine, /^# (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
			printf "\033[36m%-22s\033[0m %s\n", helpCommand,helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help
