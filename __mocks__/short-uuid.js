let counter = 0;

const generate = () => {
  const value = counter.toString().padStart(4, '0');
  counter += 1;
  return value;
};

const resetMockCounter = () => {
  counter = 0;
};

export { generate, resetMockCounter };
export default { generate, resetMockCounter };
