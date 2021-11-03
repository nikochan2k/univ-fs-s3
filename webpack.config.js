module.exports = {
  mode: "development",
  entry: {
    "basic.spec": "./src/__tests__/basic.spec.ts",
    "head.spec": "./src/__tests__/head.spec.ts",
    "list.spec": "./src/__tests__/list.spec.ts",
  },
  output: {
    filename: "[name].js",
    path: __dirname + "/dist",
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: "ts-loader",
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"],
    fallback: {
      stream: false,
    },
  },
};
