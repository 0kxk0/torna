# Torna build wrapper.
# Source layout: src/<program>/<program>.c  (program = directory name)
# Output:        out/<program>.so          + out/<program>-keypair.json
#
# Override SBF_SDK if your Solana install lives elsewhere.

SBF_SDK ?= $(HOME)/.local/share/solana/install/active_release/bin/platform-tools-sdk/sbf

include $(SBF_SDK)/c/sbf.mk
