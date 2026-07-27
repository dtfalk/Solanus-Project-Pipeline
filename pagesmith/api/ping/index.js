// Keep alive function
module.exports = async function (context, req) {
    context.log("Received ping request");
  
    context.res = {
      status: 200,
      headers: {
        "Content-Type": "text/plain"
      },
      body: "pong"
    };
  };
  