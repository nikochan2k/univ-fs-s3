module.exports = {
  mode: "development",
  entry: {
    index: "./src/index.ts",
  },
  output: {
    filename: "univ-fs-s3.js",
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
