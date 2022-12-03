CC = clang
MKDIR = mkdir
RM = rm

LIBS = fuse hiredis

LIB_FLAGS = $(addprefix -l,$(LIBS))
CDEFINES = _FILE_OFFSET_BITS=64 FUSE_USE_VERSION=26
CFLAGS = -g -Wall
CFLAGS += $(addprefix -D,$(CDEFINES))
CINCLUDES = /usr/include/linux/fuse.h

SRC_DIR = src
BUILD_DIR = build
OBJ_DIR = $(BUILD_DIR)/obj
DEP_DIR = $(BUILD_DIR)/dep

SRC_PATHS = $(wildcard $(SRC_DIR)/*.c)
OBJ_PATHS = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(SRC_PATHS))
LLVM_IR_PATHS = $(shell find . -name "*.ll")


.PHONY: all

all: $(BUILD_DIR)/redifs


$(BUILD_DIR)/redifs: $(OBJ_PATHS) | $(BUILD_DIR)
	$(CC) $^ -o $@ $(LIB_FLAGS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR) $(DEP_DIR)
	$(CC) -I $(CINCLUDES) -c $< $(CFLAGS) -M -MF $(patsubst $(OBJ_DIR)/%.o,$(DEP_DIR)/%.o.d,$@) -MT $@
	$(CC) -c $< -o $@ $(CFLAGS)
	$(CC) -I $(CINCLUDES) $(CFLAGS) -S -emit-llvm $(SRC_PATHS)

llvm_to_bin: $(LLVM_IR_PATHS)
	$(CC) $(LLVM_IR_PATHS) -o redifs_llvm $(LIB_FLAGS)

$(BUILD_DIR) $(OBJ_DIR) $(DEP_DIR):
	$(MKDIR) -p $@


.PHONY: clean

clean:
	$(RM) -rf $(BUILD_DIR)


include $(wildcard $(DEP_DIR)/*.d)

