#include <EEPROM.h>
#include <stdint.h>

typedef uint32_t vaddr_t;
typedef uint16_t vpageno_t;
typedef uint16_t vinpage_t;

enum {
  VMEM_ADDR_LEN = 32,
  VMEM_PAGENO_LEN = 16,
  VMEM_INPAGE_LEN = 16,
  VMEM_INPAGE_MASK = 0x0000FFFF,
  
  VMEM_PAGES = 8,
  VMEM_FRAMES = 3,
  VMEM_PAGE_SIZE = 128,
  VMEM_PAGENO_INVAL = UINT8_MAX,

  VMEM_EEPROM_SIZE = 1024,
};

class VMem {
public:
  static uint8_t read_byte(vaddr_t addr) {
    vpageno_t pageno = get_pageno(addr);
    vinpage_t inpage_addr = get_inpage_addr(addr);

    int16_t frame = get_frame_no(pageno);
    if (frame == -1) {
      uint16_t lru_frame = get_lru_frame();
      save_page(lru_frame);
      load_page(pageno, lru_frame);
      return frames[lru_frame][inpage_addr];
    } else {
      return frames[frame][inpage_addr];
    }
  }

  static void write_byte(vaddr_t addr, uint8_t b) {
    vpageno_t pageno = get_pageno(addr);
    vinpage_t inpage_addr = get_inpage_addr(addr);

    int16_t frame = get_frame_no(pageno);
    if (frame == -1) {
      uint16_t lru_frame = get_lru_frame();
      save_page(lru_frame);
      load_page(pageno, lru_frame);
      frames[lru_frame][inpage_addr] = b;
    } else {
      frames[frame][inpage_addr] = b;
    }
  }

private:
  static inline vpageno_t get_pageno(vaddr_t addr) {
    return addr >> VMEM_PAGENO_LEN;
  }

  static inline vinpage_t get_inpage_addr(vaddr_t addr) {
    return addr & VMEM_INPAGE_MASK;
  }

  static inline void save_page(uint16_t frame) {
    
  }

  static inline void load_page(vpageno_t page, uint16_t toframe) {
    
  }

  static inline int16_t get_frame_no(vpageno_t pageno) {
    for (size_t i = 0; i < VMEM_FRAMES; i++) {
      if (this.pageno[i] == pageno) {
        return (int16_t)i;
      }
    }
    return -1;
  }

  static inline uint16_t get_lru_frame() {
    uint64_t min_usage = UINT64_MAX;
    uint16_t min_idx = 0;
    for (size_t i = 0; i < VMEM_FRAMES; i++) {
      if (usage[i] < min_usage) {
        min_idx = i;
        min_usage = usage[i];
      }
    }

    return min_idx;
  }

  static uint8_t frames[VMEM_PAGE_SIZE][VMEM_FRAMES];
  static uint16_t pageno[VMEM_FRAMES];
  static uint64_t usage[VMEM_FRAMES];
};

void setup() {
}

void loop() {
  for (;;) {
    delay(1000);
  }
}
