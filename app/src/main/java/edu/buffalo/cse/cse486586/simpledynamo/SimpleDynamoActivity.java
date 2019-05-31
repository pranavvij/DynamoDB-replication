package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	EditText edittext;
	Button button;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    	edittext = (EditText) findViewById(R.id.edittext);
		button = (Button) findViewById(R.id.button);
		TextView tv = (TextView) findViewById(R.id.textView1);
		button.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View view) {
				ContentValues contentValues = new ContentValues();
				contentValues.put("key", "helloo");
				contentValues.put("value", "hello");
				getContentResolver().insert(Constants.getUri(), contentValues);
			}
		});
        tv.setMovementMethod(new ScrollingMovementMethod());
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
			getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

	@Override
	protected void onPause() {
		super.onPause();
	}

	public void onStop() {
        super.onStop();
	}

}
